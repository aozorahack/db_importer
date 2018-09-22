const Crawler = require('crawler');
const rp = require('request-promise');

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
    const etags = client.db().collection('ranking_etags');
    const crawler = new Crawler({
      callback: process_response,
      retries: 0,
      userAgent: 'Mozilla/5.0',
      client: client
    });

    /*
      url = 'https://www.aozora.gr.jp/access_ranking/2018_08_xhtml.html';
    */

    let promises = [];
    for(let year = 2009; year <= (new Date()).getFullYear(); year++) {
    // for(let year = 2009; year <= 2009; year++) {
      for(let month = 1; month <= 12; month++) {
      // for(let month = 1; month <= 1; month++) {
        for(let type of ['xhtml', 'txt']) {
        // for(let type of ['xhtml']) {
          const year_month = sprintf(ymfmt, year, month);
          const year_month_type = `${year_month}_${type}`;
          const url = `https://www.aozora.gr.jp/access_ranking/${year_month_type}.html`;

          promises.push(etags.findOne({year_month_type: year_month_type}).then((entry)=>{
            let options = {
              url: url,
              headers: {
                'User-Agent': 'Mozilla/5.0'
              }
            };
            if (entry) {
              options.headers['If-None-Match'] = entry.etag;
            }
            return rp.head(options);

          }).then((header)=>{
            crawler.queue({
              url: url,
              year_month: year_month,
              type: type
            });
            return etags.replaceOne({year_month_type: year_month_type},
                                    {year_month_type: year_month_type,
                                     etag: header.etag},
                                    {upsert: true});
          }).then((result)=> {
            console.log(`Updated etag: ${result.ops[0].year_month_type}`); // eslint-disable-line no-console
            return Promise.resolve()
          }).catch((error) => {
            if(error.statusCode != 304 && error.statusCode != 404) {
              console.log(error);
            }
            return Promise.resolve()
          }));
        }
      }
    }
    Promise.all(promises).then((result)=> {
      if (crawler.queueSize > 0) {
        crawler.on('drain', () => {
          client.close();
        });
      } else {
        client.close();
      }
    });
  }).catch((error)=> {
    console.error('error', error);
  });
};

run();
