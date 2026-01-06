require('dotenv').config();
const redis = require('../config/redis');
const { secondaryPool } = require('../config/db');
const { getOrCreatePlaylist, upsertListitem } = require('../lib/db-ops');

const STREAM_KEY = 'stream:watch-history';
const GROUP_NAME = 'wh-group';
const CONSUMER_NAME = 'worker-1';

async function setupConsumerGroup() {
  try {
    await redis.xgroup('CREATE', STREAM_KEY, GROUP_NAME, '$', 'MKSTREAM');
  } catch (err) {
    if (!err.message.includes('BUSYGROUP')) {
      throw err;
    }
  }
}

async function processBatch(messages) {
  const client = await secondaryPool.connect();
  try {
    await client.query('BEGIN');
    console.log(`[Worker] Starting batch of ${messages.length} messages`);

    for (const msg of messages) {
      const [id, fields] = msg;
      const data = {};
      for (let i = 0; i < fields.length; i += 2) {
        data[fields[i]] = fields[i + 1];
      }

      const { user_id, content_id, catalog_id, play_back_time } = data;
      console.log(`[Worker] Processing msg ${id}: User ${user_id}, Content ${content_id}`);

      if (!user_id || !content_id) {
        console.warn(`[Worker] Missing data in message ${id}:`, data);
        continue;
      }

      const playlistId = await getOrCreatePlaylist(client, user_id);
      console.log(`[Worker] Playlist ID: ${playlistId}`);

      await upsertListitem(client, playlistId, content_id, catalog_id, play_back_time);
      console.log(`[Worker] Upserted listitem for content ${content_id}`);
    }

    await client.query('COMMIT');
    console.log('[Worker] Batch committed successfully');
    return true;
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[Worker] Error processing batch:', err);
    return false;
  } finally {
    client.release();
  }
}

async function processStream() {
  await setupConsumerGroup();
  console.log('Worker started, listening to stream...');

  while (true) {
    try {
      const results = await redis.xreadgroup(
        'GROUP', GROUP_NAME, CONSUMER_NAME,
        'COUNT', 50,
        'BLOCK', 2000,
        'STREAMS', STREAM_KEY, '>'
      );

      if (results) {
        const [key, messages] = results[0];
        
        if (messages.length > 0) {
          console.log(`Processing batch of ${messages.length} items`);
          const success = await processBatch(messages);
          if (success) {
            const ids = messages.map(msg => msg[0]);
            await redis.xack(STREAM_KEY, GROUP_NAME, ...ids);
          }
        }
      }
    } catch (err) {
      console.error('Error in worker loop:', err);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}

processStream();
