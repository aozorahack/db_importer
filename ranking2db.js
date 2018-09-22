const Crawler = require('crawler');

require('dotenv').config();

const sprintf = require('sprintf-js').sprintf;

const mongodb = require('mongodb');
const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

const re = /.*card(\d+).html/;

const _do_process_response = async (client, res) => {
  const year_month = res.options.year_month;
  const type = res.options.type;
  const collection = client.db().collection('ranking_'+type);

  const $ = res.$;
  const bulk_ops_promise = $('tr[valign]').map(async (idx,tr) => {
    if (idx == 0) return null;
    // const rank = $(tr).find('td:nth-child(1)').text();
    const book_id = parseInt(re.exec($(tr).find('td:nth-child(2) a').attr('href'))[1]);
    const query = {year_month: year_month, book_id: book_id};

    const dbentry = await collection.findOne(query);
    if(dbentry != null) {
      return null;
    }

    const entry = {
      year_month: year_month,
      book_id: book_id,
      access : parseInt($(tr).find('td:nth-child(4)').text())
    };
    return {updateOne: {filter: query,
                        update: entry,
                        upsert: true}};
  }).get();

  return Promise.all(bulk_ops_promise).then((bops)=> {
    const bulk_ops = bops.filter((entry) => entry != null);
    if (bulk_ops.length == 0) return Promise.resolve();

    return collection.bulkWrite(bulk_ops).then((result)=> {
      console.log(`${res.options.year_month}_${res.options.type}: updated ${result.upsertedCount} entries`); // eslint-disable-line no-console
      return collection.createIndex({year_month: 1, book_id: 1}, {name: 'index'});
    }).catch((error) => {
      console.error(error);
    });
  });
};

const process_response = (error, res, done) => {
  if (error) {
    console.error(error);
  } else {
    _do_process_response(res.options.client, res).then(()=> {
      done();
    });
  }
};

const ymfmt = '%4d_%02d';

const run = () => {

  mongodb.MongoClient.connect(mongo_url, {useNewUrlParser: true}).then((client) => {
    const crawler = new Crawler({
      callback: process_response,
      retries: 0,
      userAgent: 'Mozilla/5.0',
      client: client
    });

    /*
      crawler.queue({
      url: 'https://www.aozora.gr.jp/access_ranking/2017_08_xhtml.html',
      year_month: '2017_08',
      type: 'xhtml'
      });
    */

    for(let year = 2009; year <= (new Date()).getFullYear(); year++) {
    // for(let year = 2009; year <= 2009; year++) {
      for(let month = 1; month <= 12; month++) {
        for(let type of ['xhtml', 'txt']) {
          const year_month = sprintf(ymfmt, year, month);
          const url = `https://www.aozora.gr.jp/access_ranking/${year_month}_${type}.html`;
          crawler.queue({
            url: url,
            year_month: year_month,
            type: type
          });
        }
      }
    }

    crawler.on('drain', () => {
      client.close();
    });
  }).catch((error)=> {
    console.error('error', error);
  });
};


run();
