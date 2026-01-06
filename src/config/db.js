const { Pool } = require('pg');

const masterPool = new Pool({
  connectionString: process.env.MASTER_DATABASE_URL || 'postgresql://localhost:5432/saranyu_ott_development',
  max: 10,
  idleTimeoutMillis: 30000,
});

const secondaryPool = new Pool({
  connectionString: process.env.SECONDARY_DATABASE_URL || 'postgresql://localhost:5432/saranyu_ott_development',
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

module.exports = {
  masterPool,
  secondaryPool,
};
