const SCHEMA = 'dangal_dangal_schema';

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

module.exports = {
  getOrCreatePlaylist,
  upsertListitem,
  SCHEMA,
};
