require('dotenv').config();
require('./server');
const fetch = require('node-fetch');
const { Client } = require('@notionhq/client');

const notion = new Client({ auth: process.env.NOTION_TOKEN });
const delay = ms => new Promise(res => setTimeout(res, ms));

// === RETRY + LOG EVERYTHING ===
async function withRetry(fn, id, maxRetries = 5) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (err) {
      console.log(`RETRY ${i + 1}/${maxRetries} for ID ${id}: ${err.message}`);
      await delay(1000 * (i + 1));
    }
  }
  console.log(`FAILED PERMANENTLY: ID ${id}`);
  return null;
}

async function sync() {
  console.log(`\nSYNC STARTED → ${new Date().toLocaleString('en-GB', { timeZone: 'Asia/Jakarta' })}`);

  try {
    // === 1. FETCH FRESH CSV (FORCE NO CACHE) ===
    const csvUrl = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQqCHazojRJAxhCRZSYti-xH1IZEcQp0syyWdxqo8OkGsPNlliPNP8LvmP4cBGwOwgLG7miuCE9fEuC/pub?output=csv&nocache=' + Date.now();
    const res = await fetch(csvUrl);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const csv = await res.text();

    const lines = csv.trim().split(/\r?\n/);
    if (lines.length <= 1) throw new Error('CSV empty');

    const rows = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;

      const cols = parseCSVLine(line);
      if (cols.length < 5) continue;

      const memberId = cols[0]?.trim();
      const firstName = cols[3]?.trim();
      const lastName = cols[4]?.trim();

      if (!memberId || !firstName || !lastName) continue;

      const fullName = `${firstName} ${lastName}`.trim();

      let phone = '';
      for (let j = 5; j < cols.length; j++) {
        let p = cols[j]?.trim().replace(/[^0-9+]/g, '');
        if (/^\+?\d{8,15}$/.test(p)) {
          if (!p.startsWith('+')) {
            if (p.startsWith('62')) p = '+' + p;
            else if (p.startsWith('0')) p = '+62' + p.slice(1);
            else p = '+62' + p;
          }
          phone = p;
          break;
        }
      }

      rows.push({ name: fullName, phone, id: memberId });
    }

    console.log(`CSV LOADED → ${rows.length} members (with phone: ${rows.filter(r => r.phone).length})`);

    // === 2. GET ALL NOTION PAGES ===
    let existing = [];
    let cursor = undefined;
    do {
      const res = await withRetry(() => notion.databases.query({
        database_id: process.env.NOTION_DB,
        start_cursor: cursor,
        page_size: 100
      }), 'DB_QUERY');
      if (res) {
        existing = existing.concat(res.results);
        cursor = res.next_cursor;
      }
      await delay(400);
    } while (cursor);

    const idToPage = new Map();
    for (const page of existing) {
      const id = page.properties["Member ID"]?.title?.[0]?.text?.content;
      if (id) idToPage.set(id, page.id);
    }
    console.log(`NOTION CURRENT → ${existing.length} pages`);

    // === 3. SYNC WITH LOGS + RETRY ===
    let updated = 0, created = 0, failed = 0;
    for (const row of rows) {
      const result = await withRetry(async () => {
        const props = {
          "First Name": { rich_text: [{ text: { content: row.name } }] },
          "Mobile Phone": row.phone ? { phone_number: row.phone } : { phone_number: null },
          "Member ID": { title: [{ text: { content: row.id } }] }
        };

        if (idToPage.has(row.id)) {
          await notion.pages.update({ page_id: idToPage.get(row.id), properties: props });
          return 'updated';
        } else {
          await notion.pages.create({ parent: { database_id: process.env.NOTION_DB }, properties: props });
          return 'created';
        }
      }, row.id);

      if (result === 'updated') updated++;
      else if (result === 'created') created++;
      else failed++;

      await delay(400); // ~2.5 req/sec
    }

    console.log(`SYNC DONE → Updated: ${updated} | Created: ${created} | Failed: ${failed}`);

    // === 4. SEND .VCF ===
    const withPhone = rows.filter(r => r.phone);
    if (withPhone.length > 0) {
      const vcf = withPhone.map(r => `BEGIN:VCARD\nVERSION:3.0\nFN:${r.name}\nTEL:${r.phone}\nEND:VCARD`).join('\n');

      await fetch('https://api.mailgun.net/v3/sandbox91df0697fa28496c9d47efec7d061a34.mailgun.org/messages', {
        method: 'POST',
        headers: {
          'Authorization': 'Basic ' + Buffer.from('api:' + process.env.MAILGUN_KEY).toString('base64'),
          'Content-Type': 'application/x-www-form-urlencoded'
        },
        body: new URLSearchParams({
          from: 'Wahaha Sync <mailgun@sandbox91df0697fa28496c9d47efec7d061a34.mailgun.org>',
          to: 'wahahaseafoodmarketing@gmail.com',
          subject: `${withPhone.length} Contacts - FRESH SYNC`,
          text: `Open .vcf → tap → ${withPhone.length} contacts imported.\n\nFRESH DATA FROM SQL EXCEL — JUST NOW.`,
          attachment: JSON.stringify({
            filename: 'wahaha-contacts.vcf',
            data: Buffer.from(vcf).toString('base64')
          })
        })
      });
      console.log('.vcf EMAILED — FRESH DATA');
    }

  } catch (err) {
    console.error('FATAL ERROR:', err.message);
  }
}

function parseCSVLine(line) {
  const result = [];
  let field = '';
  let inQuote = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i], n = line[i + 1];
    if (c === '"' && inQuote && n === '"') { field += '"'; i++; }
    else if (c === '"') inQuote = !inQuote;
    else if (c === ',' && !inQuote) { result.push(field); field = ''; }
    else field += c;
  }
  result.push(field);
  return result.map(f => f.trim());
}

// === RUN NOW + DAILY ===
sync();
setInterval(sync, 24 * 60 * 60 * 1000);