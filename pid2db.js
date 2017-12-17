const scraperjs = require('scraperjs');

require('dotenv').config();

const mongodb = require('mongodb');
const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

const scrape_url = (idurl) => {
  return scraperjs.StaticScraper.create(idurl)
    .scrape(($) => {
      return $('tr[valign]').map(function() {
        return {id: $(this).find(':nth-child(1)').text().trim(),
                name: $(this).find(':nth-child(2)').text().trim().replace('　',' ')};
      }).get();
    })
    .then((items) => {
      return items.slice(1);
    });
};

const idurls = {
  // 'persons': 'http://reception.aozora.gr.jp/pidlist.php?page=1&pagerow=-1',
  'workers': 'http://reception.aozora.gr.jp/widlist.php?page=1&pagerow=-1'
};

const run = async () => {
  const db = await mongodb.MongoClient.connect(mongo_url);
  for(let idname in idurls) {
    const idurl = idurls[idname];
    const bulk_ops = (await scrape_url(idurl)).map((item) => {
      item.id = parseInt(item.id);
      return {updateOne: {filter: {id: item.id},
                          update: item,
                          upsert: true}};
    });
    console.log(`updated ${bulk_ops.length} entries`); // eslint-disable-line no-console
    await db.collection(idname).bulkWrite(bulk_ops);
  }
  db.close();
};

run();
