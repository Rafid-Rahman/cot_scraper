const puppeteer = require('puppeteer');
const { google } = require('googleapis');

function getColumnName(n) {
  let s = '';
  while (n > 0) {
    let r = (n - 1) % 26;
    s = String.fromCharCode(65 + r) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

const CREDS_PATH = 'F:/Upwork/creds.json';
const SPREADSHEET_ID = '11bcHqLaR6Of0c-c1LzX_wCHwbA9fSXI5wgDctysV9Ss';
const SHEET_NAME = 'Final Dashboard';
const SHEET_ID = '657769042';

const monthCodes = {
  F: 'Jan', G: 'Feb', H: 'Mar', J: 'Apr', K: 'May',
  M: 'Jun', N: 'Jul', Q: 'Aug', U: 'Sep', V: 'Oct',
  X: 'Nov', Z: 'Dec',
};

const commoditySymbols = [
  'CC','CL','CT','GC','GF','HE','HG','HO','KC','LB','LE','NG',
  'OJ','PA','PL','RB','SB','SI','ZC','ZL','ZM','ZO','ZR','ZS','ZW'
];

function parseContractCode(contract) {
  if (contract.includes('Cash')) {
    return { symbol: contract.slice(0, 2), contractMonth: '', dateObj: null };
  }
  const symbol = contract.slice(0, 2);
  const monthCode = contract.slice(2, 3);
  const year = '20' + contract.slice(3, 5);
  const month = monthCodes[monthCode] || '';
  const dateObj = new Date(`${year}-${String(Object.keys(monthCodes).indexOf(monthCode) + 1).padStart(2, '0')}-01`);
  return { symbol, contractMonth: `${month} ${year}`, dateObj };
}

function parsePrice(raw) {
  if (raw.includes('-')) {
    const [whole, frac] = raw.split('-');
    return parseFloat(whole) + (parseInt(frac, 10) / 8);
  }
  return parseFloat(raw.replace(',', '.'));
}

function getColumnRange(index) {
  const startIndex = index * 7; // 6 columns + 1 gap
  const start = getColumnName(startIndex + 1);
  const end = getColumnName(startIndex + 6);
  return `${start}2:${end}15`;
}

async function authorizeGoogle() {
  const auth = new google.auth.GoogleAuth({
    keyFile: CREDS_PATH,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  return await auth.getClient();
}

async function insertRows(sheets) {
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [
        {
          insertDimension: {
            range: {
              sheetId: SHEET_ID,
              dimension: 'ROWS',
              startIndex: 1,
              endIndex: 15,
            },
            inheritFromBefore: false,
          },
        },
      ],
    },
  });
}

(async () => {
  let browser = null;
  try {
    const auth = await authorizeGoogle();
    const sheets = google.sheets({ version: 'v4', auth });

    browser = await puppeteer.launch({ headless: false });
    const page = await browser.newPage();

    const symbolDataMap = {};

    for (const symbol of commoditySymbols) {
      const url = `https://www.barchart.com/futures/quotes/${symbol}*0/futures-prices?timeFrame=daily`;
      console.log(`Navigating to ${url}`);
      await page.goto(url, { waitUntil: 'networkidle2', timeout: 60000 });

      const frontContractCode = await page.evaluate(() => {
        const h1 = document.querySelector('h1.inline-block span:nth-child(2)');
        return h1 ? h1.textContent.replace(/[()]/g, '').trim() : null;
      });

      if (!frontContractCode) continue;
      const { dateObj: frontDate } = parseContractCode(frontContractCode);
      if (!frontDate) continue;

      const cutoff = new Date(frontDate);
      cutoff.setMonth(cutoff.getMonth() + 11);

      const gridBox = await page.$('bc-data-grid');
      const boundingBox = await gridBox.boundingBox();
      for (let i = 0; i < 20; i++) {
        await page.mouse.move(boundingBox.x + boundingBox.width / 2, boundingBox.y + boundingBox.height / 2);
        await page.mouse.wheel({ deltaY: 150 });
        await new Promise(res => setTimeout(res, 700));
      }

      const gridHandle = await page.$('bc-data-grid');
      const gridShadowRoot = await gridHandle.evaluateHandle(el => el.shadowRoot);
      const rowHandles = await gridShadowRoot.$$(`set-class.row`);

      const collectedRows = [];
      for (const rowHandle of rowHandles) {
        const contract = await rowHandle.evaluate(row => {
          const cell = row.querySelector('div.contractSymbol text-binding');
          return cell && cell.shadowRoot ? cell.shadowRoot.textContent.trim() : '';
        });
        if (!contract) continue;

        const { contractMonth, dateObj } = parseContractCode(contract);
        if (!contract.includes('Cash')) {
          if (dateObj < frontDate || dateObj > cutoff) continue;
        }

        const priceRaw = await rowHandle.evaluate(row => {
          const cell = row.querySelector('div.dailyLastPrice text-binding');
          return cell && cell.shadowRoot ? cell.shadowRoot.textContent.trim() : '';
        });
        const openInterest = await rowHandle.evaluate(row => {
          const cell = row.querySelector('div.dailyOpenInterest text-binding');
          return cell && cell.shadowRoot ? cell.shadowRoot.textContent.trim() : '';
        });
        const time = await rowHandle.evaluate(row => {
          const cell = row.querySelector('div.dailyDate1dAgo text-binding');
          return cell && cell.shadowRoot ? cell.shadowRoot.textContent.trim() : '';
        });

        collectedRows.push([
          contract,
          symbol,
          contractMonth,
          parsePrice(priceRaw),
          openInterest,
          time,
        ]);

        await new Promise(res => setTimeout(res, 1000));
      }

      symbolDataMap[symbol] = collectedRows;
    }

    await insertRows(sheets); // Insert 14 rows at top

    // Write to each commodity block
    for (let i = 0; i < commoditySymbols.length; i++) {
      const symbol = commoditySymbols[i];
      const range = getColumnRange(i);
      const rows = symbolDataMap[symbol] || [];
      const padded = rows.concat(new Array(13 - rows.length).fill(['', '', '', '', '', '']));

      await sheets.spreadsheets.values.update({
        spreadsheetId: SPREADSHEET_ID,
        range: `${SHEET_NAME}!${range}`,
        valueInputOption: 'USER_ENTERED',
        requestBody: { values: padded },
      });
    }

    console.log(`✅ Successfully inserted all data in block format.`);

  } catch (err) {
    console.error('❌ Error:', err);
  } finally {
    if (browser) await browser.close();
  }
})();
