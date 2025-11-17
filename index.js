require('dotenv').config();
require('./server');
const fetch = require('node-fetch');
const { Client } = require('@notionhq/client');
const FormData = require('form-data');

const notion = new Client({ auth: process.env.NOTION_TOKEN });
const delay = ms => new Promise(res => setTimeout(res, ms));

async function withRetry(fn, id) {
  for (let i = 0; i < 5; i++) {
    try { return await fn(); }
    catch (err) {
      console.log(`Retry ${i+1} for ${id}: ${err.message}`);
      await delay(1000 * (i + 1));
    }
  }
  console.error(`Failed after 5 retries for ${id}`);
  return null;
}

// First, let's check what properties actually exist in your Notion database
async function checkNotionProperties() {
  try {
    console.log('🔍 Checking Notion database properties...');
    const database = await notion.databases.retrieve({
      database_id: process.env.NOTION_DB
    });
    
    console.log('📋 Available properties in your Notion database:');
    Object.keys(database.properties).forEach(propName => {
      const prop = database.properties[propName];
      console.log(`   - "${propName}" (type: ${prop.type})`);
    });
    
    return database.properties;
  } catch (error) {
    console.error('❌ Failed to fetch Notion database properties:', error.message);
    return null;
  }
}

// Simple CSV parser
function parseCSV(text) {
  const rows = [];
  const lines = text.split('\n').filter(line => line.trim());
  
  for (const line of lines) {
    const fields = [];
    let currentField = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];
      
      if (char === '"') {
        if (inQuotes && nextChar === '"') {
          currentField += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (char === ',' && !inQuotes) {
        fields.push(currentField.trim());
        currentField = '';
      } else {
        currentField += char;
      }
    }
    
    fields.push(currentField.trim());
    if (fields.length > 1 && fields.some(field => field && field !== '""')) {
      rows.push(fields);
    }
  }
  
  return rows;
}

// MUCH BETTER phone cleaning with debugging
function cleanPhoneNumber(phone) {
  if (!phone || phone.trim() === '') return '';
  
  // Remove all non-digit characters except +
  let cleaned = phone.toString().replace(/[^\d+]/g, '');
  
  // If empty after cleaning, return empty
  if (!cleaned) return '';
  
  // Debug: log what we're cleaning
  if (phone !== cleaned) {
    console.log(`   🔧 Phone cleaning: "${phone}" → "${cleaned}"`);
  }
  
  // Handle various Indonesian phone formats
  if (cleaned.startsWith('0')) {
    cleaned = '+62' + cleaned.substring(1);
  } else if (cleaned.startsWith('62') && !cleaned.startsWith('+')) {
    cleaned = '+' + cleaned;
  } else if (cleaned.startsWith('8') && !cleaned.startsWith('+')) {
    cleaned = '+62' + cleaned;
  }
  
  // More lenient validation for Indonesian numbers
  // Indonesian numbers: +62 followed by 8-13 digits
  if (/^\+62\d{8,13}$/.test(cleaned)) {
    return cleaned;
  }
  
  // If it's a reasonable length but missing country code, try to fix
  if (cleaned.length >= 10 && cleaned.length <= 13 && !cleaned.startsWith('+')) {
    const fixed = '+62' + cleaned;
    if (/^\+62\d{8,13}$/.test(fixed)) {
      console.log(`   ✅ Fixed phone: "${phone}" → "${fixed}"`);
      return fixed;
    }
  }
  
  console.log(`   ❌ Invalid phone: "${phone}" → "${cleaned}"`);
  return '';
}

async function sync() {
  console.log(`\n🚀 SYNC STARTED → ${new Date().toLocaleString('en-GB', { timeZone: 'Asia/Jakarta' })}`);

  try {
    // First, check what properties exist in Notion
    const notionProperties = await checkNotionProperties();
    if (!notionProperties) {
      throw new Error('Cannot access Notion database properties');
    }

    // Determine the correct property names
    const propertyMap = {
      id: 'Member ID',
      name: Object.keys(notionProperties).find(name => 
        name.toLowerCase().includes('name') || name.toLowerCase().includes('first')
      ),
      phone: Object.keys(notionProperties).find(name => 
        name.toLowerCase().includes('phone') || name.toLowerCase().includes('mobile')
      )
    };

    console.log('🎯 Using property mapping:', propertyMap);

    if (!propertyMap.name) {
      throw new Error('No name property found in Notion database');
    }

    const csvUrl = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQqCHazojRJAxhCRZSYti-xH1IZEcQp0syyWdxqo8OkGsPNlliPNP8LvmP4cBGwOwgLG7miuCE9fEuC/pub?output=csv&nocache=' + Date.now();
    console.log('📥 Fetching CSV from Google Sheets...');
    
    const res = await fetch(csvUrl);
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
    const csv = await res.text();

    console.log('📊 CSV received, length:', csv.length);
    
    const data = parseCSV(csv);
    console.log(`📈 Parsed ${data.length} total rows from CSV`);

    if (data.length <= 1) throw new Error('CSV empty or only headers');

    const header = data[0];
    console.log('🔍 First 10 CSV Headers:', header.slice(0, 10));

    // Map columns based on the actual CSV structure
    const findColumn = (patterns) => {
      for (let pattern of patterns) {
        const index = header.findIndex(h => 
          h && typeof h === 'string' && h.toLowerCase().includes(pattern.toLowerCase())
        );
        if (index !== -1) return index;
      }
      return -1;
    };

    const col = {
      id: findColumn(['membershipid', 'member', 'id']),
      title: findColumn(['mtitle', 'title']),
      firstName: findColumn(['firstname', 'first', 'name']),
      lastName: findColumn(['lastname', 'last']),
      phone: findColumn(['telephone', 'phone', 'mobile', 'mmobphone'])
    };

    console.log('🎯 Column mapping:', col);

    const rows = [];
    let validPhones = 0;
    let skippedRows = 0;
    let phoneDebugShown = false;

    console.log('\n🔧 Processing rows...');
    
    for (let i = 1; i < data.length; i++) {
      const cols = data[i];
      
      if (!cols || cols.length < 4) {
        skippedRows++;
        continue;
      }

      // Extract data with proper fallbacks
      const id = col.id !== -1 && cols[col.id] ? cols[col.id].trim() : `MEM${i}`;
      const title = col.title !== -1 ? cols[col.title]?.trim() : '';
      const firstName = col.firstName !== -1 ? cols[col.firstName]?.trim() : '';
      const lastName = col.lastName !== -1 ? cols[col.lastName]?.trim() : '';
      
      // Combine names properly
      const fullName = [title, firstName, lastName].filter(Boolean).join(' ').trim();
      
      // Get phone from the telephone column
      const phoneRaw = col.phone !== -1 ? cols[col.phone]?.trim() : '';
      const phone = cleanPhoneNumber(phoneRaw);

      // Debug: Show phone processing for first few rows
      if (i <= 10 && phoneRaw && !phoneDebugShown) {
        console.log(`   🔍 Debug phone ${i}: Raw="${phoneRaw}" → Cleaned="${phone}"`);
        if (i === 10) phoneDebugShown = true;
      }

      if (!fullName) {
        skippedRows++;
        continue;
      }

      if (phone) validPhones++;

      rows.push({ id, name: fullName, phone });
    }

    console.log(`\n📊 PROCESSING SUMMARY:`);
    console.log(`✅ Total members processed: ${rows.length}`);
    console.log(`📞 Members with valid phones: ${validPhones}`);
    console.log(`❌ Skipped invalid rows: ${skippedRows}`);
    
    // Show members WITH phones for debugging
    const membersWithPhones = rows.filter(r => r.phone);
    console.log(`\n📱 Members WITH valid phones (first 10):`);
    if (membersWithPhones.length > 0) {
      membersWithPhones.slice(0, 10).forEach((row, i) => {
        console.log(`  ${i+1}. ID: ${row.id}, Name: ${row.name}, Phone: ${row.phone}`);
      });
    } else {
      console.log('  No members with valid phones found');
    }

    console.log('\n👥 All sample members (first 10):');
    rows.slice(0, 10).forEach((row, i) => {
      console.log(`  ${i+1}. ID: ${row.id}, Name: ${row.name}, Phone: ${row.phone || 'N/A'}`);
    });

    // === UPDATE/CREATE IN NOTION ===
    console.log('\n🔄 Syncing with Notion...');
    let created = 0;
    let updated = 0;
    let failed = 0;

    // Process in smaller batches
    const batchSize = 30;
    for (let b = 0; b < rows.length; b += batchSize) {
      const batch = rows.slice(b, b + batchSize);
      const batchNum = Math.floor(b/batchSize) + 1;
      const totalBatches = Math.ceil(rows.length/batchSize);
      
      console.log(`\n📦 Processing batch ${batchNum}/${totalBatches} (${batch.length} members)...`);
      
      for (const row of batch) {
        try {
          // Search for existing member by ID
          const existing = await withRetry(() => notion.databases.query({
            database_id: process.env.NOTION_DB,
            filter: {
              property: "Member ID",
              title: { equals: row.id }
            },
            page_size: 1
          }), row.id);

          // Build properties dynamically based on what exists
          const properties = {
            "Member ID": { 
              title: [{ type: "text", text: { content: row.id } }] 
            }
          };

          // Add name property with correct property name
          if (propertyMap.name) {
            properties[propertyMap.name] = { 
              rich_text: [{ type: "text", text: { content: row.name } }] 
            };
          }

          // Add phone property if it exists and we have a phone number
          if (propertyMap.phone && row.phone) {
            properties[propertyMap.phone] = { phone_number: row.phone };
          }

          let result;
          if (existing && existing.results.length > 0) {
            // Update existing
            const pageId = existing.results[0].id;
            result = await withRetry(() => notion.pages.update({
              page_id: pageId,
              properties
            }), row.id);
            if (result) {
              updated++;
              console.log(`   ✏️ Updated: ${row.id} - ${row.name} ${row.phone ? '📱' : ''}`);
            }
          } else {
            // Create new
            result = await withRetry(() => notion.pages.create({
              parent: { database_id: process.env.NOTION_DB },
              properties
            }), row.id);
            if (result) {
              created++;
              console.log(`   🆕 Created: ${row.id} - ${row.name} ${row.phone ? '📱' : ''}`);
            }
          }

          if (!result) {
            failed++;
            console.log(`   ❌ Failed: ${row.id}`);
          }

          await delay(400);
          
        } catch (error) {
          failed++;
          console.error(`   💥 Error processing ${row.id}:`, error.message);
        }
      }
      
      if (batchNum < totalBatches) {
        console.log(`   ⏳ Batch ${batchNum} complete. Taking short break...`);
        await delay(1000);
      }
    }

    console.log(`\n🎉 NOTION SYNC COMPLETE:`);
    console.log(`🆕 Created: ${created}`);
    console.log(`✏️  Updated: ${updated}`);
    console.log(`❌ Failed: ${failed}`);
    console.log(`📊 Total processed: ${created + updated}`);
    console.log(`📊 Expected in CSV: ${rows.length} members`);
    console.log(`📱 Members with phones: ${validPhones}`);

    // === SEND .VCF CONTACTS ===
    const withPhone = rows.filter(r => r.phone);
    if (withPhone.length > 0 && process.env.MAILGUN_KEY) {
      console.log(`\n📧 Preparing VCF with ${withPhone.length} contacts...`);
      
      const vcf = withPhone.map(r => 
        `BEGIN:VCARD\nVERSION:3.0\nFN:${r.name}\nTEL:${r.phone}\nEND:VCARD`
      ).join('\n');

      const form = new FormData();
      form.append('from', 'Wahaha Sync <mailgun@sandbox91df0697fa28496c9d47efec7d061a34.mailgun.org>');
      form.append('to', 'wahahaseafoodmarketing@gmail.com');
      form.append('subject', `${withPhone.length} Wahaha Contacts - Daily Sync`);
      form.append('text', `Attached: ${withPhone.length} contacts from ${rows.length} total members.\n\nSync completed: ${new Date().toLocaleString('en-GB', { timeZone: 'Asia/Jakarta' })}`);
      form.append('attachment', Buffer.from(vcf), { 
        filename: `wahaha-${rows.length}-members-${withPhone.length}-contacts.vcf` 
      });

      try {
        const mailRes = await fetch('https://api.mailgun.net/v3/sandbox91df0697fa28496c9d47efec7d061a34.mailgun.org/messages', {
          method: 'POST',
          headers: {
            'Authorization': 'Basic ' + Buffer.from('api:' + process.env.MAILGUN_KEY).toString('base64')
          },
          body: form
        });

        if (mailRes.ok) {
          console.log(`✅ VCF emailed with ${withPhone.length} contacts`);
        } else {
          console.error(`❌ Mailgun error: ${mailRes.status}`);
        }
      } catch (mailError) {
        console.error('❌ Mailgun sending failed:', mailError.message);
      }
    } else if (withPhone.length > 0) {
      console.log('ℹ️  Mailgun not configured, skipping VCF email');
    } else {
      console.log('ℹ️  No valid phone numbers for VCF');
    }

    console.log('\n✨ SYNC COMPLETED SUCCESSFULLY');
    console.log(`⏰ Next run: ${new Date(Date.now() + 24 * 60 * 60 * 1000).toLocaleString('en-GB', { timeZone: 'Asia/Jakarta' })}`);

  } catch (err) {
    console.error('💥 FATAL SYNC ERROR:', err.message);
    console.error(err.stack);
  }
}

// Run immediately and then every 24 hours
console.log('🚀 Starting Wahaha Sync Service...');
sync();
setInterval(sync, 24 * 60 * 60 * 1000);