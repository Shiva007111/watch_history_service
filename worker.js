require('dotenv').config();
const redis = require('./redis-client');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://localhost:5432/saranyu_ott_development',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

const STREAM_KEY = 'stream:watch-history';
const GROUP_NAME = 'wh-group';
const CONSUMER_NAME = 'worker-1';
const SCHEMA = 'dangal_dangal_schema';

async function setupConsumerGroup() {
  try {
    await redis.xgroup('CREATE', STREAM_KEY, GROUP_NAME, '$', 'MKSTREAM');
  } catch (err) {
    if (!err.message.includes('BUSYGROUP')) {
      throw err;
    }
  }
}

async function getOrCreatePlaylist(client, userId) {
  const res = await client.query(
    `SELECT id FROM ${SCHEMA}.playlists WHERE user_id = $1 AND playlist_type = 'watchhistory' LIMIT 1`,
    [userId]
  );

  if (res.rows.length > 0) {
    return res.rows[0].id;
  }


  const randomId = Math.floor(100000000 + Math.random() * 900000000);

  const insertRes = await client.query(
    `INSERT INTO ${SCHEMA}.playlists (user_id, playlist_type, name, playlist_id, created_at, updated_at)
     VALUES ($1, 'watchhistory', 'Watch History', $2, NOW(), NOW())
     RETURNING id`,
    [userId, randomId.toString()]
  );
  return insertRes.rows[0].id;
}

async function upsertListitem(client, playlistId, contentId, catalogId, playBackTime) {
  const res = await client.query(
    `SELECT id FROM ${SCHEMA}.listitems 
     WHERE playlist_id = $1 AND content_id = $2 LIMIT 1`,
    [playlistId, contentId]
  );

  if (res.rows.length > 0) {
    await client.query(
      `UPDATE ${SCHEMA}.listitems 
       SET play_back_time = $1, updated_at = NOW()
       WHERE id = $2`,
      [playBackTime, res.rows[0].id]
    );
  } else {

    const listitemId = require('crypto').randomUUID();
    
    await client.query(
      `INSERT INTO ${SCHEMA}.listitems (playlist_id, content_id, catalog_id, play_back_time, listitem_id, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, NOW(), NOW())`,
      [playlistId, contentId, catalogId, playBackTime, listitemId]
    );
  }
}

async function processBatch(messages) {
  const client = await pool.connect();
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
