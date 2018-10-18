const rp = require('request-promise');
const cheerio = require('cheerio');
const commandLineArgs = require('command-line-args');

require('dotenv').config();

const base_url = 'https://www.aozora.gr.jp/access_ranking/';

const re = /.*card(\d+).html/;

const process_response = ($, year_month, book_ids) => {
  return $('tr[valign]').map((idx,tr) => {
    if (idx == 0) return null;

    // const rank = $(tr).find('td:nth-child(1)').text();
    const book_id = parseInt(re.exec($(tr).find('td:nth-child(2) a').attr('href'))[1]);

    if(book_ids.includes(book_id)) return null;

    const query = {year_month: year_month, book_id: book_id};
    const entry = {
      year_month: year_month,
      book_id: book_id,
      access : parseInt($(tr).find('td:nth-child(4)').text())
    };
    return {updateOne: {filter: query,
                        update: entry,
                        upsert: true}};
  }).get();
};

const transform_top = (body) => {
  const $ = cheerio.load(body);
  return $('a').map((idx, a)=>{
    const hrefre = /(\d+)_(\d+)_(.+)\.html/.exec($(a).attr('href'));
    if(hrefre) {
      return {
        year_month: hrefre[1] + '_' + hrefre[2],
        type: hrefre[3],
        url: base_url + hrefre.input
      };
      // const url = 'https://www.aozora.gr.jp/access_ranking/2018_08_xhtml.html';
    } else {
      return null;
    }
  }).get();
};

const import_to_db = async (db) => {
  const base_options = {
    headers: {
      'User-Agent': 'Mozilla/5.0'
    },
    forever: true,
    pool: {
      maxSockets: 1
    }
  };
  const options = Object.assign({
    url: base_url,
    transform: transform_top
  }, base_options);

  const entries = await rp.get(options);
  const plist = entries.map(async (e) => {
    const year_month = e.year_month;
    const type = e.type;
    const url = e.url;
    const year_month_type = `${year_month}_${type}`;
    const etags = db.collection('ranking_etags');
    const ranking = db.collection('ranking_'+type);

    let options = Object.assign({
      url: url,
      resolveWithFullResponse: true,
    }, base_options);
    const book_ids = await ranking.find(
      {year_month: year_month}, {_id:0, book_id:1}
    ).map((e) => e.book_id).toArray();
    let etag = null;
    const entry = etags.findOne({year_month_type: year_month_type});
    if (entry) {
      options.headers['If-None-Match'] = entry.etag;
    }

    try {
      const res = await rp.get(options);
      etag = res.headers.etag;
      const bulk_ops = process_response(cheerio.load(res.body),
                                        year_month, book_ids);
      if (bulk_ops.length == 0) return null;
      let result = ranking.bulkWrite(bulk_ops);

      console.log(`${year_month}_${type}: updated ${result.upsertedCount} entries`); // eslint-disable-line no-console
      await ranking.createIndex({year_month: 1, book_id: 1}, {name: 'index'});

      result = await etags.replaceOne(
        {year_month_type: year_month_type},
        {year_month_type: year_month_type, etag: etag},
        {upsert: true});

      console.log(`Updated etag: ${result.ops[0].year_month_type}`); // eslint-disable-line no-console
    } catch (error){
      if (error.statusCode == 404) {
        console.error(error.response.statusMessage + ': ' + error.options.url);
      } else if(error.statusCode != 304) {
        console.error(error);
      }
    }
  });
  return Promise.all(plist);
};

const run = async () => {
  const option_defs = [
    { name: 'backend', alias: 'b', type: String, defaultValue: 'mongo' },
  ];
  const options = commandLineArgs(option_defs);

  const DB = require('./db_' + options.backend).DB;

  const db = new DB();
  await db.connect();
  await import_to_db(db);
  await db.close();
};

run();
