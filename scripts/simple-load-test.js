const http = require('http');

const TOTAL_REQUESTS = 500;
const CONCURRENCY = 10;

function makeRequest(id) {
  return new Promise((resolve, reject) => {
    const sessionId = '3803f783bbd5fe1825f3a95c272e29e7'; // User provided session ID
    // const contentId = `content_${Math.floor(Math.random() * 100)}`;
    
    const data = JSON.stringify({
      auth_token: "test_token",
      region: "IN",
      listitem: {
        content_id: "6888c582621f8ea6e6b6c2b2",
        catalog_id: "645c9a721e7171304ea7f16f",
        play_back_time: "00:00:10"
      }
    });

    const options = {
      hostname: 'localhost',
      port: 3000,
      path: `/users/${sessionId}/playlists/watchhistory`,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': data.length
      }
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => {
        if (res.statusCode === 200 || res.statusCode === 401) {
          resolve({ status: res.statusCode });
        } else {
          reject(new Error(`Status ${res.statusCode}: ${body}`));
        }
      });
    });

    req.on('error', (e) => reject(e));
    req.write(data);
    req.end();
  });
}

async function run() {
  console.log(`Starting load test: ${TOTAL_REQUESTS} requests...`);
  let completed = 0;
  let success = 0;
  let failed = 0;

  for (let i = 0; i < TOTAL_REQUESTS; i += CONCURRENCY) {
    const batch = [];
    for (let j = 0; j < CONCURRENCY && i + j < TOTAL_REQUESTS; j++) {
      batch.push(makeRequest(i + j));
    }

    const results = await Promise.allSettled(batch);
    results.forEach(r => {
      if (r.status === 'fulfilled') {
        success++;
      } else {
        console.error('Request failed:', r.reason.message);
        failed++;
      }
      completed++;
    });
    console.log(`Progress: ${completed}/${TOTAL_REQUESTS}`);
  }

  console.log('Load test completed.');
  console.log(`Success: ${success}`);
  console.log(`Failed: ${failed}`);
}

run();
