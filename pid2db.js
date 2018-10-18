const rp = require('request-promise');
const cheerio = require('cheerio');
const commandLineArgs = require('command-line-args');

require('dotenv').config();

const transform = (body) => {
  const $ = cheerio.load(body);
  const items = $('tr[valign]').map(function() {
    return {id: parseInt($(this).find(':nth-child(1)').text().trim()),
            name: $(this).find(':nth-child(2)').text().trim().replace('　',' ')};
  }).get().slice(1);

  return items.map((item)=> {
    return {updateOne: {filter: {id: item.id},
                        update: item,
                        upsert: true}};
  });
};

const idurls = {
  // 'persons': 'http://reception.aozora.gr.jp/pidlist.php?page=1&pagerow=-1',
  'workers': 'http://reception.aozora.gr.jp/widlist.php?page=1&pagerow=-1'
};

const import_to_db = async (db) => {
  for(let idname in idurls) {
    const idurl = idurls[idname];
    const options = {
      url: idurl,
      transform: transform
    };
    const bulk_ops = await rp.get(options);
    const result = await db.collection(idname).bulkWrite(bulk_ops);
    console.log(`updated ${result.upsertedCount} entries`); // eslint-disable-line no-console
  }
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
