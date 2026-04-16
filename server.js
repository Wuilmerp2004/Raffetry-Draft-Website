import express from 'express';
import https from 'https';
import { fileURLToPath } from 'url';
import path from 'path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app  = express();
const PORT = process.env.PORT || 3000;
const GEMINI_MODEL = 'gemini-2.5-flash';

// How many videos to upload+analyze at the same time.
// Free tier: 10 req/min → keep at 3. Paid tier: bump to 8–10.
const CONCURRENCY = 8;

// ── Static frontend ───────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));

// ── Helpers ───────────────────────────────────────────────────────────────────

function httpsRequest(options, body = null) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({
        status:  res.statusCode,
        headers: res.headers,
        body:    Buffer.concat(chunks),
      }));
    });
    req.on('error', reject);
    req.setTimeout(300_000, () => { req.destroy(); reject(new Error('Timeout')); });
    if (body) req.write(body);
    req.end();
  });
}

const ANALYSIS_PROMPT = `You are a professional video archivist for RaffertyWeiss Media (RWM).
Analyze this video and return ONLY valid JSON with these exact keys — no markdown, no extra text:
{
  "title": "concise title (max 10 words)",
  "description": "2-3 sentence summary of content and purpose",
  "duration_seconds": 0,
  "content_type": "TV Commercial | PSA | Corporate Video | Virtual Conference | Training | Promotional | Documentary | Event Coverage | Interview | Other",
  "primary_subject": "single most important topic",
  "secondary_subjects": "pipe-separated or None",
  "industry_sector": "Healthcare | Finance | Education | Nonprofit | Government | Technology | Retail | Entertainment | Sports | Other",
  "target_audience": "General Public | B2B Decision Makers | Youth | Seniors | Employees | Adults",
  "tone": "Inspirational | Informational | Humorous | Serious | Emotional | Urgent | Neutral",
  "visual_style": "Live Action | Animation | Motion Graphics | Talking Head | B-Roll Montage | Mixed | Screen Recording",
  "location_type": "Indoor | Outdoor | Studio | Virtual/CGI | Mixed | Unknown",
  "has_people": "Yes or No",
  "age_groups_present": "pipe-separated or None",
  "languages_spoken": "pipe-separated or None",
  "has_captions": "Yes | No | Unknown",
  "music_present": "Yes or No",
  "logo_or_brand_visible": "Yes or No",
  "call_to_action": "exact CTA text or None",
  "keywords": "10-15 pipe-separated keywords",
  "decade_or_era": "1990s | 2000s | 2010s | 2020s | Unknown",
  "quality_rating": "Low | Medium | High",
  "notes": "notable observations or None"
}`;

function parseAIResponse(raw) {
  let text = raw.trim().replace(/^```json\s*/i, '').replace(/^```\s*/i, '').replace(/```\s*$/, '').trim();
  try { return JSON.parse(text); } catch (_) {}
  const s = text.indexOf('{'), e = text.lastIndexOf('}');
  if (s !== -1 && e > s) { try { return JSON.parse(text.slice(s, e + 1)); } catch (_) {} }
  throw new Error('Could not parse Gemini JSON response');
}

// ── Core: tag a single video buffer ──────────────────────────────────────────
async function tagVideo(apiKey, filename, mime, fileBuffer) {
  // 1. Initiate resumable upload
  const initBody = JSON.stringify({ file: { display_name: filename } });
  const initResp = await httpsRequest({
    hostname: 'generativelanguage.googleapis.com',
    path: `/upload/v1beta/files?uploadType=resumable&key=${apiKey}`,
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(initBody),
      'X-Goog-Upload-Protocol': 'resumable',
      'X-Goog-Upload-Command': 'start',
      'X-Goog-Upload-Header-Content-Length': fileBuffer.length,
      'X-Goog-Upload-Header-Content-Type': mime,
    },
  }, initBody);

  if (initResp.status !== 200)
    throw new Error(`Upload init failed (${initResp.status}): ${initResp.body.toString()}`);

  const uploadUrl = initResp.headers['x-goog-upload-url'];
  if (!uploadUrl) throw new Error('Gemini did not return an upload URL');

  // 2. Upload bytes
  const uploadUrlParsed = new URL(uploadUrl);
  const uploadResp = await httpsRequest({
    hostname: uploadUrlParsed.hostname,
    path:     uploadUrlParsed.pathname + uploadUrlParsed.search,
    method:   'POST',
    headers: {
      'Content-Length': fileBuffer.length,
      'Content-Type':   mime,
      'X-Goog-Upload-Offset':  '0',
      'X-Goog-Upload-Command': 'upload, finalize',
    },
  }, fileBuffer);

  if (uploadResp.status !== 200)
    throw new Error(`Upload failed (${uploadResp.status}): ${uploadResp.body.toString()}`);

  let fileInfo = JSON.parse(uploadResp.body.toString()).file;

  // 3. Poll until ACTIVE — 1s interval (down from 3s)
  let attempts = 0;
  while (fileInfo.state === 'PROCESSING' && attempts < 120) {
    await new Promise(r => setTimeout(r, 1000));
    const pollResp = await httpsRequest({
      hostname: 'generativelanguage.googleapis.com',
      path:     `/v1beta/${fileInfo.name}?key=${apiKey}`,
      method:   'GET',
      headers:  { 'Content-Type': 'application/json' },
    });
    fileInfo = JSON.parse(pollResp.body.toString());
    attempts++;
  }
  if (fileInfo.state === 'FAILED')
    throw new Error(`Gemini failed to process ${filename}`);

  // 4. Analyze — maxOutputTokens 2048 is plenty for JSON, responds faster
  let aiData = null, lastErr = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    const genBody = JSON.stringify({
      contents: [{
        parts: [
          { file_data: { mime_type: fileInfo.mimeType || mime, file_uri: fileInfo.uri } },
          { text: ANALYSIS_PROMPT },
        ]
      }],
      generationConfig: { temperature: 0.2, maxOutputTokens: 2048 },
    });

    const genResp = await httpsRequest({
      hostname: 'generativelanguage.googleapis.com',
      path:     `/v1beta/models/${GEMINI_MODEL}:generateContent?key=${apiKey}`,
      method:   'POST',
      headers: {
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(genBody),
      },
    }, genBody);

    if (genResp.status !== 200) {
      lastErr = `Gemini generateContent failed (${genResp.status}): ${genResp.body.toString()}`;
      await new Promise(r => setTimeout(r, 3000));
      continue;
    }

    const rawText = JSON.parse(genResp.body.toString())?.candidates?.[0]?.content?.parts?.[0]?.text || '';
    try { aiData = parseAIResponse(rawText); break; }
    catch (e) { lastErr = e.message; await new Promise(r => setTimeout(r, 3000)); }
  }

  // 5. Delete from Gemini (fire and forget)
  httpsRequest({
    hostname: 'generativelanguage.googleapis.com',
    path:     `/v1beta/${fileInfo.name}?key=${apiKey}`,
    method:   'DELETE',
    headers:  { 'Content-Type': 'application/json' },
  }).catch(() => {});

  if (!aiData) throw new Error(lastErr || 'Failed to get AI response');
  return aiData;
}

// ── Multipart parser ──────────────────────────────────────────────────────────
function splitBuffer(buf, delimiter) {
  const parts = [];
  let start = 0;
  while (true) {
    const idx = buf.indexOf(delimiter, start);
    if (idx === -1) { parts.push(buf.slice(start)); break; }
    parts.push(buf.slice(start, idx));
    start = idx + delimiter.length;
  }
  return parts.filter(p => p.length > 4);
}

function parseMultipart(rawBody, boundary) {
  const fields = {};
  let fileBuffer = null;
  const parts = splitBuffer(rawBody, Buffer.from('\r\n' + boundary));
  for (const part of parts) {
    const headerEnd = part.indexOf('\r\n\r\n');
    if (headerEnd === -1) continue;
    const headerStr = part.slice(0, headerEnd).toString();
    const bodyBuf   = part.slice(headerEnd + 4);
    const body = bodyBuf.slice(-2).toString() === '\r\n' ? bodyBuf.slice(0, -2) : bodyBuf;
    const nameMatch = headerStr.match(/name="([^"]+)"/);
    if (!nameMatch) continue;
    if (nameMatch[1] === 'file') fileBuffer = body;
    else fields[nameMatch[1]] = body.toString().trim();
  }
  return { fields, fileBuffer };
}

// ── POST /api/tag — single video ──────────────────────────────────────────────
app.post('/api/tag', async (req, res) => {
  try {
    const contentType  = req.headers['content-type'] || '';
    const boundaryMatch = contentType.match(/boundary=(.+)$/);
    if (!boundaryMatch) return res.status(400).json({ error: 'Expected multipart/form-data' });

    const rawBody = await new Promise((resolve, reject) => {
      const chunks = [];
      req.on('data', c => chunks.push(c));
      req.on('end',  () => resolve(Buffer.concat(chunks)));
      req.on('error', reject);
    });

    const { fields, fileBuffer } = parseMultipart(rawBody, '--' + boundaryMatch[1]);
    const { apiKey, filename, mimetype } = fields;
    if (!apiKey || !fileBuffer || !filename)
      return res.status(400).json({ error: 'Missing apiKey, file, or filename' });

    const mime   = mimetype || 'video/mp4';
    const aiData = await tagVideo(apiKey, filename, mime, fileBuffer);

    res.json({ ok: true, aiData, fileSizeMb: (fileBuffer.length / 1024 / 1024).toFixed(2) });
  } catch (err) {
    console.error('/api/tag error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/tag-batch — multiple videos in parallel ────────────────────────
// Body: JSON { apiKey, files: [{ filename, mimetype, dataBase64 }, ...] }
// Returns a stream of newline-delimited JSON (one result per line as they finish)
app.post('/api/tag-batch', express.json({ limit: '500mb' }), async (req, res) => {
  const { apiKey, files } = req.body;
  if (!apiKey || !Array.isArray(files) || !files.length)
    return res.status(400).json({ error: 'Missing apiKey or files array' });

  // Stream results back as newline-delimited JSON so the UI can update live
  res.setHeader('Content-Type', 'application/x-ndjson');
  res.setHeader('Transfer-Encoding', 'chunked');
  res.flushHeaders();

  // Run up to CONCURRENCY jobs at once
  const queue = [...files];
  let active  = 0;
  let done    = 0;

  await new Promise(resolve => {
    function next() {
      while (active < CONCURRENCY && queue.length) {
        const f = queue.shift();
        active++;
        const buf = Buffer.from(f.dataBase64, 'base64');

        tagVideo(apiKey, f.filename, f.mimetype || 'video/mp4', buf)
          .then(aiData => {
            res.write(JSON.stringify({
              ok:          true,
              filename:    f.filename,
              fileSizeMb:  (buf.length / 1024 / 1024).toFixed(2),
              aiData,
            }) + '\n');
          })
          .catch(err => {
            res.write(JSON.stringify({
              ok:       false,
              filename: f.filename,
              error:    err.message,
            }) + '\n');
          })
          .finally(() => {
            active--;
            done++;
            if (queue.length) next();
            else if (active === 0) resolve();
          });
      }
    }
    next();
  });

  res.end();
});

// ── Catch-all → index.html ────────────────────────────────────────────────────
app.get('*', (_, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`\n  RWM Video Archive`);
  console.log(`  http://localhost:${PORT}`);
  console.log(`  Concurrency: ${CONCURRENCY} videos at once`);
  console.log(`  Press Ctrl+C to stop\n`);
});
