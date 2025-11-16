require('dotenv').config();
require('./server');
const fetch = require('node-fetch');
const { Client } = require('@notionhq/client');

const notion = new Client({ auth: process.env.NOTION_TOKEN });
const delay = ms => new Promise(res => setTimeout(res, ms));

// === RETRY HELPER ===
async function withRetry(fn, maxRetries = 5) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (err) {
      if (err.message.includes('ECONNRESET') || err.code === 'ECONNRESET') {
        console.log(`Connection reset. Retry ${i + 1}/${maxRetries}...`);
        await delay(1000 * (i + 1));
      } else {
        throw err;
      }
    }
  }
  throw new Error('Max retries exceeded');
}

async function sync() {
  console.log(`\nSYNC STARTED → ${new Date().toLocaleString('en-GB', { timeZone: 'Asia/Jakarta' })}`);

  try {
    const csvUrl = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQqCHazojRJAxhCRZSYti-xH1IZEcQp0syyWdxqo8OkGsPNlliPNP8LvmP4cBGwOwgLG7miuCE9fEuC/pub?output=csv';
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

      rows.push([fullName, phone, memberId]);
    }

    console.log(`Found ${rows.length} members (with phone: ${rows.filter(r => r[1]).length})`);

    // === GET EXISTING PAGES (with retry + delay) ===
    let existing = [];
    let cursor = undefined;
    do {
      await withRetry(async () => {
        const res = await notion.databases.query({
          database_id: process.env.NOTION_DB,
          start_cursor: cursor,
          page_size: 100
        });
        existing = existing.concat(res.results);
        cursor = res.next_cursor;
      });
      await delay(400);
    } while (cursor);

    const idToPageId = new Map();
    for (const page of existing) {
      const id = page.properties["Member ID"]?.title?.[0]?.text?.content;
      if (id) idToPageId.set(id, page.id);
    }

    // === UPDATE/CREATE WITH RETRY + DELAY ===
    let updated = 0, created = 0;
    for (const [name, phone, id] of rows) {
      const pageId = idToPageId.get(id);

      const props = {
        "First Name": { rich_text: [{ text: { content: name } }] },
        "Mobile Phone": phone ? { phone_number: phone } : { phone_number: null },
        "Member ID": { title: [{ text: { content: id } }] }
      };

      await withRetry(async () => {
        if (pageId) {
          await notion.pages.update({ page_id: pageId, properties: props });
          updated++;
        } else {
          await notion.pages.create({ parent: { database_id: process.env.NOTION_DB }, properties: props });
          created++;
        }
      });

      await delay(400); // ~2.5 req/sec = 100% safe
    }

    console.log(`NOTION DONE → Updated: ${updated} | Created: ${created}`);

    // === SEND .VCF ===
    const withPhone = rows.filter(r => r[1]);
    if (withPhone.length > 0) {
      const vcf = withPhone.map(([n, p]) => `BEGIN:VCARD\nVERSION:3.0\nFN:${n}\nTEL:${p}\nEND:VCARD`).join('\n');

      await fetch('https://api.mailgun.net/v3/sandbox91df0697fa28496c9d47efec7d061a34.mailgun.org/messages', {
        method: 'POST',
        headers: {
          'Authorization': 'Basic ' + Buffer.from('api:' + process.env.MAILGUN_KEY).toString('base64'),
          'Content-Type': 'application/x-www-form-urlencoded'
        },
        body: new URLSearchParams({
          from: 'Wahaha Sync <mailgun@sandbox91df0697fa28496c9d47efec7d061a34.mailgun.org>',
          to: 'wahahaseafoodmarketing@gmail.com',
          subject: `${withPhone.length} Contacts - Import Now`,
          text: `Open .vcf on phone → tap → ${withPhone.length} contacts appear.`,
          attachment: JSON.stringify({
            filename: 'wahaha-contacts.vcf',
            data: Buffer.from(vcf).toString('base64')
          })
        })
      });
      console.log('.vcf emailed');
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

// === RUN ===
sync();
setInterval(sync, 24 * 60 * 60 * 1000);