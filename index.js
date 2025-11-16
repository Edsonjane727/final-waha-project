require('dotenv').config();
require('./server');
const fetch = require('node-fetch');
const { Client } = require('@notionhq/client');

const notion = new Client({ auth: process.env.NOTION_TOKEN });

async function sync() {
  console.log(`\nSYNC STARTED → ${new Date().toLocaleString('en-GB', { timeZone: 'Asia/Jakarta' })}`);

  try {
    const csvUrl = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQqCHazojRJAxhCRZSYti-xH1IZEcQp0syyWdxqo8OkGsPNlliPNP8LvmP4cBGwOwgLG7miuCE9fEuC/pub?output=csv';
    const res = await fetch(csvUrl);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const csv = await res.text();

    const lines = csv.trim().split(/\r?\n/);
    const rows = [];

    for (let i = 1; i < lines.length; i++) {
      const cols = parseCSVLine(lines[i]);
      if (cols.length < 5) continue;

      const id = cols[0]?.trim();
      const first = cols[3]?.trim();
      const last = cols[4]?.trim();
      let phone = cols[13]?.trim() || '';

      if (!id || !first || !last) continue;

      // CLEAN PHONE IF EXISTS
      if (phone) {
        phone = phone.replace(/[^0-9+]/g, '');
        if (!phone.startsWith('+')) {
          if (phone.startsWith('62')) phone = '+' + phone;
          else if (phone.startsWith('0')) phone = '+62' + phone.slice(1);
          else phone = '+62' + phone;
        }
      }

      const name = `${first} ${last}`.trim();
      if (name) {
        rows.push([name, phone, id]);
      }
    }

    console.log(`Found ${rows.length} members (phones: ${rows.filter(r => r[1]).length})`);

    // NOTION
    let existing = [];
    let cursor;
    do {
      const r = await notion.databases.query({
        database_id: process.env.NOTION_DB,
        start_cursor: cursor
      });
      existing.push(...r.results);
      cursor = r.next_cursor;
    } while (cursor);

    const map = new Map(existing.map(p => [p.properties["Member ID"]?.title?.[0]?.text?.content, p.id]));

    let updated = 0, created = 0;
    for (const [name, phone, id] of rows) {
      const pageId = map.get(id);
      const props = {
        "First Name": { rich_text: [{ text: { content: name } }] },
        "Mobile Phone": phone ? { phone_number: phone } : { phone_number: null },
        "Member ID": { title: [{ text: { content: id } }] }
      };

      try {
        if (pageId) {
          await notion.pages.update({ page_id: pageId, properties: props });
          updated++;
        } else {
          await notion.pages.create({ parent: { database_id: process.env.NOTION_DB }, properties: props });
          created++;
        }
      } catch (e) {
        console.log(`Notion error for ID ${id}: ${e.message}`);
      }
    }

    console.log(`NOTION DONE → Updated: ${updated} | Created: ${created}`);

    // SEND .vcf (ONLY WITH PHONE)
    const vcfRows = rows.filter(r => r[1]);
    const vcf = vcfRows.map(([name, phone]) =>
      `BEGIN:VCARD\nVERSION:3.0\nFN:${name}\nTEL:${phone}\nEND:VCARD`
    ).join('\n');

    await fetch('https://api.mailgun.net/v3/sandbox91df0697fa28496c9d47efec7d061a34.mailgun.org/messages', {
      method: 'POST',
      headers: {
        'Authorization': 'Basic ' + Buffer.from('api:' + process.env.MAILGUN_KEY).toString('base64'),
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      body: new URLSearchParams({
        from: 'Wahaha Sync <mailgun@sandbox91df0697fa28496c9d47efec7d061a34.mailgun.org>',
        to: 'wahahaseafoodmarketing@gmail.com',
        subject: `${vcfRows.length} Contacts - Import Now`,
        text: `Open .vcf on phone → tap → ${vcfRows.length} contacts appear.`,
        attachment: JSON.stringify({
          filename: 'wahaha-contacts.vcf',
          data: Buffer.from(vcf).toString('base64')
        })
      })
    });

    console.log('Contacts .vcf emailed');
  } catch (e) {
    console.error('FATAL ERROR:', e.message);
  }
}

function parseCSVLine(line) {
  const result = [];
  let field = '';
  let inQuote = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];

    if (char === '"') {
      if (inQuote && nextChar === '"') {
        field += '"';
        i++;
      } else {
        inQuote = !inQuote;
      }
    } else if (char === ',' && !inQuote) {
      result.push(field.trim());
      field = '';
    } else {
      field += char;
    }
  }
  result.push(field.trim());
  return result;
}

sync();
setInterval(sync, 24 * 60 * 60 * 1000);