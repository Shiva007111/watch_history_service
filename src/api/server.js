require('dotenv').config();
const fastify = require('fastify')({ logger: true });
const { updateWatchHistory, watchHistorySchema } = require('../controllers/watch-history.controller');
const { getWatchHistory } = require('../controllers/get-watch-history.controller');

// Routes
fastify.post('/users/:session_id/playlists/watchhistory', { schema: watchHistorySchema }, updateWatchHistory);
fastify.get('/users/:user_id/watchhistory', getWatchHistory);

// Health check
fastify.get('/health', async () => {
  return { status: 'ok', uptime: process.uptime() };
});

const start = async () => {
  try {
    const port = process.env.PORT || 3000;
    await fastify.listen({ port: parseInt(port), host: '0.0.0.0' });
    console.log(`Server listening on port ${port}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
