// server.js — dummy server to keep Render happy
require('http')
  .createServer((req, res) => {
    res.writeHead(200);
    res.end('Sync running');
  })
  .listen(10000);

console.log('Dummy server on port 10000');
