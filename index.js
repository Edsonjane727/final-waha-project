require('dotenv').config();
require('./server');
const fetch = require('node-fetch');
const { Client } = require('@notionhq/client');

const notion = new Client({ auth: process.env.NOTION_TOKEN });

// === DELAY HELPER ===
const delay = ms => new Promise(res => setTimeout(res, ms));

async function sync() {
  console.log(`\nSYNC STARTED → ${new Date().toLocaleString('en-GB', { timeZone: 'Asia/Jakarta' })}`);

  try {
    const csvUrl = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQqCHazojRJAxhCRZSYti-xH1IZEcQp0syyWdxqo8OkGsPNlliPNP8LvmP4cBGwOwgLG7miuCE9fEuC/pub?output=csv';
    const res = await fetch(csvUrl);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const csv = await res.text();

    const lines = csv.trim().split(/\r?\n/);
    if (lines.length <= 1) throw new Error('CSV is empty');

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
        let candidate = cols[j]?.trim();
        if (!candidate) continue;
        candidate = candidate.replace(/[^0-9+]/g, '');
        if (/^\+?\d{8,15}$/.test(candidate)) {
          if (!candidate.startsWith('+')) {
            if (candidate.startsWith('62')) candidate = '+' + candidate;
            else if (candidate.startsWith('0')) candidate = '+62' + candidate.slice(1);
            else candidate = '+62' + candidate;
          }
          phone = candidate;
          break;
        }
      }

      rows.push([fullName, phone, memberId]);
    }

    console.log(`Found ${rows.length} members (with phone: ${rows.filter(r => r[1]).length})`);

    // === GET EXISTING PAGES ===
    let existing = [];
    let cursor = undefined;
    do {
      const response = await notion.databases.query({
        database_id: process.env.NOTION_DB,
        start_cursor: cursor,
        page_size: 100
      });
      existing = existing.concat(response.results);
      cursor = response.next_cursor;
      await delay(350); // Respect rate limit
    } while (cursor);

    const idToPageId = new Map();
    for (const page of existing) {
      const titleText = page.properties["Member ID"]?.title?.[0]?.text?.content;
      if (titleText) idToPageId.set(titleText, page.id);
    }

    // === UPDATE/CREATE WITH DELAY ===
    let updated = 0, created = 0;
    for (const [name, phone, id] of rows) {
      const pageId = idToPageId.get(id);

      const properties = {
        "First Name": { rich_text: [{ text: { content: name } }] },
        "Mobile Phone": phone ? { phone_number: phone } : { phone_number: null },
        "Member ID": { title: [{ text: { content: id } }] }
      };

      try {
        if (pageId) {
          await notion.pages.update({ page_id: pageId, properties });
          updated++;
        } else {
          await notion.pages.create({
            parent: { database_id: process.env.NOTION_DB },
            properties
          });
          created++;
        }
        await delay(350); // 3 requests per second = safe
      } catch (err) {
        console.log(`Notion error for ID ${id}:`, err.message);
        await delay(1000); // Wait longer on error
      }
    }

    console.log(`NOTION DONE → Updated: ${updated} | Created: ${created}`);

    // === SEND .VCF ===
    const contactsWithPhone = rows.filter(r => r[1]);
    if (contactsWithPhone.length > 0) {
      const vcf = contactsWithPhone.map(([name, phone]) =>
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
          subject: `${contactsWithPhone.length} Contacts - Import Now`,
          text: `Open .vcf on phone → tap → ${contactsWithPhone.length} contacts appear.`,
          attachment: JSON.stringify({
            filename: 'wahaha-contacts.vcf',
            data: Buffer.from(vcf).toString('base64')
          })
        })
      });

      console.log('.vcf emailed');
    }

  } catch (error) {
    console.error('FATAL ERROR:', error.message);
  }
}

function parseCSVLine(line) {
  const result = [];
  let field = '';
  let inQuote = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"') {
      if (inQuote && next === '"') {
        field += '"';
        i++;
      } else {
        inQuote = !inQuote;
      }
    } else if (char === ',' && !inQuote) {
      result.push(field);
      field = '';
    } else {
      field += char;
    }
  }
  result.push(field);
  return result.map(f => f.trim());
}

sync();
setInterval(sync, 24 * 60 * 60 * 1000);