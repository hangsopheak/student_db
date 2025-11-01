import fs from 'fs';
import path from 'path';
import jsonServer from 'json-server';
import fetch from 'node-fetch';
import { put, list } from '@vercel/blob';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const server = jsonServer.create();
const middlewares = jsonServer.defaults();
const port = process.env.PORT || 3000;

const BLOB_FOLDER = 'db';
const API_MODE = 'read'; // "read" or "crud"
console.log(`🔧 API Mode: ${API_MODE}`);

// Rate limiting
const RATE_LIMIT = 30;
const rateLimitWindow = 60 * 1000;
const ipRequests = new Map();

// ✅ OPTIMIZATION 1: Cache routers per database
const routerCache = new Map();
const lastSaveTime = new Map();
const DEBOUNCE_DELAY = 1000; // Wait 1 second before saving to Blob

function rateLimit(req, res) {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  const now = Date.now();

  if (!ipRequests.has(ip)) {
    ipRequests.set(ip, []);
  }

  const timestamps = ipRequests.get(ip).filter(ts => now - ts < rateLimitWindow);
  timestamps.push(now);
  ipRequests.set(ip, timestamps);

  if (timestamps.length > RATE_LIMIT) {
    return false;
  }
  return true;
}

function isValidGUID(guid) {
  const guidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  return guidRegex.test(guid);
}

async function getDBFromBlob(dbName) {
  try {
    const prefix = `${BLOB_FOLDER}/${dbName}`;
    const blobs = await list({ 
      prefix: prefix, 
      token: process.env.BLOB_READ_WRITE_TOKEN,
      limit: 100 
    });
    
    if (blobs.blobs.length > 0) {
      const latestBlob = blobs.blobs
        .filter(blob => blob.pathname.includes(`${dbName}.json`))
        .sort((a, b) => new Date(b.uploadedAt) - new Date(a.uploadedAt))[0];
      
      if (latestBlob) {
        console.log(`Loading latest DB version: ${latestBlob.pathname}`);
        const res = await fetch(latestBlob.url);
        if (res.ok) {
          return await res.json();
        }
      }
    }
    return null;
  } catch (error) {
    console.log(`No existing DB found for ${dbName}:`, error.message);
    return null;
  }
}

// ✅ OPTIMIZATION 2: Debounced save to prevent excessive Blob writes
const savePending = new Map();

async function saveDBToBlob(dbName, data) {
  const key = `${BLOB_FOLDER}/${dbName}.json`;
  
  try {
    const blob = await put(key, JSON.stringify(data, null, 2), {
      access: 'public',
      token: process.env.BLOB_READ_WRITE_TOKEN,
      contentType: 'application/json',
      addRandomSuffix: false
    });
    
    console.log(`Saved DB ${dbName} to blob: ${blob.pathname}`);
    lastSaveTime.set(dbName, Date.now());
    return blob;
  } catch (error) {
    console.error(`Failed to save DB ${dbName}:`, error);
    throw error;
  }
}

function debouncedSave(dbName, router) {
  // Clear existing timeout
  if (savePending.has(dbName)) {
    clearTimeout(savePending.get(dbName));
  }

  // Set new timeout
  const timeoutId = setTimeout(async () => {
    try {
      await saveDBToBlob(dbName, router.db.getState());
      savePending.delete(dbName);
    } catch (error) {
      console.error(`Failed to save changes for DB ${dbName}:`, error);
    }
  }, DEBOUNCE_DELAY);

  savePending.set(dbName, timeoutId);
}

// ✅ OPTIMIZATION 3: Initialize router once per database
async function getOrCreateRouter(dbName) {
  if (routerCache.has(dbName)) {
    return routerCache.get(dbName);
  }

  let dbData;
  
  if (API_MODE === 'read') {
    const templatePath = path.join(__dirname, 'template.json');
    dbData = fs.existsSync(templatePath)
      ? JSON.parse(fs.readFileSync(templatePath, 'utf8'))
      : {};
    console.log(`Loaded DB ${dbName} in READ mode from template`);
  } else {
    dbData = await getDBFromBlob(dbName);
    if (!dbData) {
      const templatePath = path.join(__dirname, 'template.json');
      dbData = fs.existsSync(templatePath)
        ? JSON.parse(fs.readFileSync(templatePath, 'utf8'))
        : {};
      console.log(`Initialized DB ${dbName} from template`);
      await saveDBToBlob(dbName, dbData);
    }
  }

  const router = jsonServer.router(dbData);
  routerCache.set(dbName, router);
  
  return router;
}

server.use(middlewares);

server.use((req, res, next) => {
  if (!rateLimit(req, res)) {
    return res.status(429).json({ error: 'Too many requests. Please try again later.' });
  }
  next();
});

server.use(async (req, res, next) => {
  const dbName = req.header('X-DB-NAME');
  
  if (!dbName) {
    return res.status(400).json({ error: 'X-DB-NAME header is required' });
  }
  
  if (!isValidGUID(dbName)) {
    return res.status(400).json({ error: 'X-DB-NAME must be a valid GUID' });
  }

  if (API_MODE === 'read' && ['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) {
    return res.status(403).json({ error: 'API is in READ-ONLY mode. Write operations are disabled.' });
  }
  
  try {
    const router = await getOrCreateRouter(dbName);

    // ✅ OPTIMIZATION 4: Use debounced save instead of saving on every request
    if (API_MODE === 'crud' && ['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) {
      res.on('finish', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          debouncedSave(dbName, router);
        }
      });
    }

    router(req, res, next);
  } catch (error) {
    console.error(`Error handling request for DB ${dbName}:`, error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ✅ OPTIMIZATION 5: Cleanup on shutdown
process.on('SIGTERM', async () => {
  console.log('Shutting down gracefully...');
  
  // Save all pending changes
  for (const [dbName, timeoutId] of savePending.entries()) {
    clearTimeout(timeoutId);
    const router = routerCache.get(dbName);
    if (router) {
      await saveDBToBlob(dbName, router.db.getState());
    }
  }
  
  process.exit(0);
});

server.listen(port, () => {
  console.log(`JSON Server running on port ${port}`);
});