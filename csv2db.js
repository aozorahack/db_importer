const request = require('request-promise');
const AdmZip = require('adm-zip');
const csvparse = require('csv-parse/lib/sync');

const DB = require('./db_mongo').DB;
const ATTRS = require('./attrs').ATTRS;

const LIST_URL_BASE = 'https://github.com/aozorabunko/aozorabunko/raw/master/index_pages/';
const LIST_URL_PUB = 'list_person_all_extended_utf8.zip';

const ROLE_MAP = {
  '著者': 'authors',
  '翻訳者': 'translators',
  '編者': 'editors',
  '校訂者': 'revisers'
};

const get_csv_data = async (local_file) => {
  let csvtext;
  if(local_file) {
    const fs = require('fs');
    csvtext = fs.readFileSync(local_file);
  } else {
    csvtext = await request.get(LIST_URL_BASE + LIST_URL_PUB, {encoding: null});
  }
  const zip = AdmZip(csvtext);
  return zip.readFile(zip.getEntries()[0]);
};

const type_conversion = (data) => {
  return ATTRS.map((e,i) => {
    const value = data[i];
    if(['copyright', 'author_copyright'].includes(e)) {
      return value != 'なし';
    } else if(['release_date', 'last_modified', 'date_of_birth', 'date_of_death',
               'text_last_modified', 'html_last_modified'].includes(e)) {
      return new Date(value);
    } else if('role' == e) {
      return ROLE_MAP[value];
    } else {
      return value;
    }
  });
}

const import_to_db = async (db, refresh) => {
  refresh = refresh || false;

  // const zfile = 'x10.zip';
  // const zfile = 'list_person_all_extended_utf8.zip';
  const zfile = undefined;

  const data = csvparse(await get_csv_data(zfile), {auto_parse: true, from: 2})
        .map(type_conversion);

  const updated = refresh? data: await db.updated(data);

  if(updated.length > 0) {
    const count = await db.import(data);
    console.log(`${count} entries are updated`);  // eslint-disable-line no-console
  } else {
    console.log(`zero entries are updated`);  // eslint-disable-line no-console
  }
};

const run = async () => {
  const db = new DB();
  await db.connect();
  const refresh = process.argv[2];
  await import_to_db(db, refresh);
  await db.close();
};

run();
