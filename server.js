import express from 'express';
import http from 'http';
import https from 'https';
import { execFile } from 'child_process';
import { tmpdir } from 'os';
import { writeFile, readFile, unlink } from 'fs/promises';
import { randomBytes } from 'crypto';
import { fileURLToPath } from 'url';
import path from 'path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app  = express();
const PORT = process.env.PORT || 3000;
const GEMINI_MODEL = 'gemini-2.5-flash';
const CONCURRENCY  = 5;

// ── Static frontend ───────────────────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));

// ── HTTPS helper ──────────────────────────────────────────────────────────────
function httpsRequest(options, body = null) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    req.setTimeout(600_000, () => { req.destroy(); reject(new Error('Timeout')); });
    if (body) req.write(body);
    req.end();
  });
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
  const files  = [];
  const parts  = splitBuffer(rawBody, Buffer.from('\r\n' + boundary));
  for (const part of parts) {
    const headerEnd = part.indexOf('\r\n\r\n');
    if (headerEnd === -1) continue;
    const headerStr = part.slice(0, headerEnd).toString();
    const bodyBuf   = part.slice(headerEnd + 4);
    const body      = bodyBuf.slice(-2).toString() === '\r\n' ? bodyBuf.slice(0, -2) : bodyBuf;
    const nameMatch = headerStr.match(/name="([^"]+)"/);
    if (!nameMatch) continue;
    const name     = nameMatch[1];
    const fileMatch = headerStr.match(/filename="([^"]+)"/);
    if (fileMatch) {
      const mimeMatch = headerStr.match(/Content-Type:\s*([^\r\n]+)/i);
      files.push({ fieldname: name, filename: fileMatch[1], mimetype: (mimeMatch?.[1] || 'video/mp4').trim(), buffer: body });
    } else {
      fields[name] = body.toString().trim();
    }
  }
  return { fields, files };
}

// ── FFmpeg compression ────────────────────────────────────────────────────────
async function ffmpegAvailable() {
  return new Promise(resolve => execFile('ffmpeg', ['-version'], err => resolve(!err)));
}
async function compressVideo(inputBuffer, mime) {
  const id         = randomBytes(8).toString('hex');
  const ext        = mime.includes('quicktime') ? 'mov' : 'mp4';
  const inPath     = path.join(tmpdir(), `rwm_in_${id}.${ext}`);
  const outPath    = path.join(tmpdir(), `rwm_out_${id}.mp4`);
  const sizeMb     = inputBuffer.length / 1024 / 1024;
  const resolution = sizeMb > 100 ? '240' : '360';
  const fps        = sizeMb > 100 ? '5' : '15';
  const crf        = sizeMb > 100 ? '32' : '28';

  await writeFile(inPath, inputBuffer);
  await new Promise((resolve, reject) => {
    execFile('ffmpeg', [
      '-i', inPath,
      '-vf', `scale=-2:${resolution}`,
      '-c:v', 'libx264', '-crf', crf, '-preset', 'ultrafast',
      '-r', fps,
      '-c:a', 'aac', '-b:a', '64k',
      '-movflags', '+faststart',
      '-y', outPath
    ], { timeout: 300_000 }, err => err ? reject(err) : resolve());
  });
  const compressed = await readFile(outPath);
  await unlink(inPath).catch(() => {});
  await unlink(outPath).catch(() => {});
  return compressed;
}

// ── Gemini prompt ─────────────────────────────────────────────────────────────
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

// ── Core: tag one video buffer ────────────────────────────────────────────────
async function tagVideo(apiKey, filename, mime, fileBuffer) {
  // Compress if ffmpeg available and file > 50MB
  const sizeMb = fileBuffer.length / 1024 / 1024;
  if (sizeMb > 50) {
    const hasFfmpeg = await ffmpegAvailable();
    if (hasFfmpeg) {
      console.log(`[${filename}] Compressing ${sizeMb.toFixed(0)}MB → 360p…`);
      try {
        fileBuffer = await compressVideo(fileBuffer, mime);
        mime = 'video/mp4';
        console.log(`[${filename}] Compressed to ${(fileBuffer.length/1024/1024).toFixed(0)}MB`);
      } catch (e) {
        console.warn(`[${filename}] Compression failed, uploading original: ${e.message}`);
      }
    }
  }

  // 1. Initiate resumable upload
  const initBody = JSON.stringify({ file: { display_name: filename } });
  const initResp = await httpsRequest({
    hostname: 'generativelanguage.googleapis.com',
    path:     `/upload/v1beta/files?uploadType=resumable&key=${apiKey}`,
    method:   'POST',
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
  const up = new URL(uploadUrl);
  const uploadResp = await httpsRequest({
    hostname: up.hostname,
    path:     up.pathname + up.search,
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

  const uploadJson = JSON.parse(uploadResp.body.toString());
  let fileInfo = uploadJson.file || uploadJson;

  // 3. Poll until ACTIVE
  let attempts = 0;
  while (fileInfo.state === 'PROCESSING' && attempts < 240) {
    await new Promise(r => setTimeout(r, 500));
    const pollResp = await httpsRequest({
      hostname: 'generativelanguage.googleapis.com',
      path:     `/v1beta/${fileInfo.name}?key=${apiKey}`,
      method:   'GET',
      headers:  { 'Content-Type': 'application/json' },
    });
    const pollJson = JSON.parse(pollResp.body.toString());
    fileInfo = pollJson.file || pollJson;
    attempts++;
  }
  if (fileInfo.state === 'FAILED')
    throw new Error(`Gemini failed to process ${filename}`);

  // Validate URI
  console.log(`[${filename}] fileInfo:`, JSON.stringify({ name: fileInfo.name, uri: fileInfo.uri, state: fileInfo.state, mimeType: fileInfo.mimeType }));
  if (!fileInfo.uri || !fileInfo.uri.startsWith('https://')) {
    throw new Error(`Gemini returned invalid file URI: "${fileInfo.uri}"`);
  }

  // 4. Analyze
  let aiData = null, lastErr = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    const genBody = JSON.stringify({
      contents: [{ parts: [
        { file_data: { mime_type: fileInfo.mimeType || mime, file_uri: fileInfo.uri } },
        { text: ANALYSIS_PROMPT },
      ]}],
      generationConfig: { temperature: 0.2, maxOutputTokens: 4096 },
    });
    const genResp = await httpsRequest({
      hostname: 'generativelanguage.googleapis.com',
      path:     `/v1beta/models/${GEMINI_MODEL}:generateContent?key=${apiKey}`,
      method:   'POST',
      headers:  { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(genBody) },
    }, genBody);

    if (genResp.status !== 200) {
      lastErr = `Gemini error (${genResp.status}): ${genResp.body.toString()}`;
      await new Promise(r => setTimeout(r, 3000));
      continue;
    }
    const rawText = JSON.parse(genResp.body.toString())?.candidates?.[0]?.content?.parts?.[0]?.text || '';
    console.log(`[${filename}] Gemini response:\n${rawText}\n`);
    try { aiData = parseAIResponse(rawText); break; }
    catch (e) { lastErr = e.message; await new Promise(r => setTimeout(r, 3000)); }
  }

  // 5. Delete from Gemini
  httpsRequest({
    hostname: 'generativelanguage.googleapis.com',
    path:     `/v1beta/${fileInfo.name}?key=${apiKey}`,
    method:   'DELETE',
    headers:  { 'Content-Type': 'application/json' },
  }).catch(() => {});

  if (!aiData) throw new Error(lastErr || 'Failed to get AI response');
  return aiData;
}

// ── POST /api/tag-batch ───────────────────────────────────────────────────────
// Receives all videos as multipart/form-data in one request.
// Streams back NDJSON as each video finishes.
app.post('/api/tag-batch', (req, res) => {
  const contentType   = req.headers['content-type'] || '';
  const boundaryMatch = contentType.match(/boundary=(.+)$/);
  if (!boundaryMatch) return res.status(400).json({ error: 'Expected multipart/form-data' });

  res.setHeader('Content-Type', 'application/x-ndjson');
  res.setHeader('Transfer-Encoding', 'chunked');
  res.flushHeaders();

  const chunks = [];
  req.on('data', c => chunks.push(c));
  req.on('error', err => { res.write(JSON.stringify({ ok: false, error: err.message }) + '\n'); res.end(); });
  req.on('end', async () => {
    const rawBody = Buffer.concat(chunks);
    const { fields, files } = parseMultipart(rawBody, '--' + boundaryMatch[1]);
    const totalMb = (rawBody.length / 1024 / 1024).toFixed(1);
    const usedMb = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
    const rss = (process.memoryUsage().rss / 1024 / 1024).toFixed(1);
    console.log(`Upload received: ${totalMb}MB — Heap: ${usedMb}MB — RSS: ${rss}MB`);
    const apiKey = fields.apiKey;

    if (!apiKey || !files.length) {
      res.write(JSON.stringify({ ok: false, error: 'Missing apiKey or files' }) + '\n');
      return res.end();
    }

    // Process up to CONCURRENCY at once
    const queue  = [...files];
    let active   = 0;
    let total    = files.length;
    let finished = 0;

    await new Promise(resolve => {
      function next() {
        while (active < CONCURRENCY && queue.length) {
          const f = queue.shift();
          active++;
          tagVideo(apiKey, f.filename, f.mimetype, f.buffer)
            .then(aiData => {
              res.write(JSON.stringify({
                ok: true, filename: f.filename,
                fileSizeMb: (f.buffer.length / 1024 / 1024).toFixed(2),
                aiData,
              }) + '\n');
            })
            .catch(err => {
              res.write(JSON.stringify({ ok: false, filename: f.filename, error: err.message }) + '\n');
            })
            .finally(() => {
              active--;
              finished++;
              if (queue.length) next();
              else if (active === 0) resolve();
            });
        }
      }
      next();
    });

    res.end();
  });
});

// ── Catch-all → index.html ────────────────────────────────────────────────────
app.get('*', (_, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

const server = http.createServer(app);
server.maxRequestsPerSocket = 0;
server.listen(PORT, () => {
  console.log(`\n  RWM Video Archive`);
  console.log(`  http://localhost:${PORT}`);
  console.log(`  Concurrency: ${CONCURRENCY} videos at once`);
  console.log(`  Press Ctrl+C to stop\n`);
});
