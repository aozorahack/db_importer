require('dotenv').config();

const pg = require('pg');

class DB {
  constructor() {
    this.client = new pg.Client({database: 'aozora', parseInputDatesAsUTC: true});
  }

  async connect() {
    return this.client.connect();
  }

  async updated(data, refresh) {
    if (refresh) {
      await this.client.query('DELETE FROM list');
      return data;
    }

    const the_latest = await this.client.query(
      'SELECT release_date FROM list ORDER BY release_date DESC LIMIT 1');

    const last_release_date = (the_latest.rowCount > 0)?
      the_latest.rows[0].release_date: new Date(null);

    return data.slice(1).filter((entry) => {
      return last_release_date < new Date(entry[11]);
    });
  }

  async import(updated) {
    return Promise.all(updated.map((line)=> {
      return this.client.query('INSERT INTO list VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55) ON CONFLICT DO NOTHING', line);
    })).then((res)=> {
      return res.map((e) => (e.rowCount)).reduce((a,b)=>(a+b), 0);
    });
  }

  async close() {
    this.client.end();
  }
}

exports.DB = DB;
