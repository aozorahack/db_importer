import commandLineArgs from 'command-line-args';
import csvparse from 'csv-parse/lib/sync';

import * as csvlib from 'csv-parse/lib';
import * as rp from 'request-promise';

import {IDB} from './i_db';

import AdmZip = require('adm-zip');

import {ATTRS} from './attrs';

const LIST_URL_BASE = 'https://github.com/aozorabunko/aozorabunko/raw/master/index_pages/';
const LIST_URL_PUB = 'list_person_all_extended_utf8.zip';

const ROLE_MAP: { [key: string]: string } = {
  校訂者: 'revisers',
  編者: 'editors',
  翻訳者: 'translators',
  著者: 'authors',
};

async function get_csv_data(local_file: string | null): Promise<string> {
  let csvtext;
  if (local_file) {
    const fs = await import('fs');
    csvtext = fs.readFileSync(local_file);
  } else {
    csvtext = await rp.get(LIST_URL_BASE + LIST_URL_PUB, {encoding: null});
  }
  const zip = new AdmZip(csvtext);
  return zip.readFile(zip.getEntries()[0]).toString();
}

function type_conversion(data: string[]) {
  return ATTRS.map((e, i) => {
    const value = data[i];
    if (['copyright', 'author_copyright'].includes(e)) {
      return value !== 'なし';
    } else if (['book_id', 'person_id', 'text_updated', 'html_updated'].includes(e)) {
      return value === '' ? -1 : parseInt(value, 10);
    } else if (['release_date', 'last_modified', 'text_last_modified',
               'html_last_modified'].includes(e)) {
      return value === '' ? null : new Date(value);
    } else if ('role' === e) {
      return ROLE_MAP[value];
    } else {
      return value;
    }
  });
}

async function import_to_db(db: IDB, refresh = false) {
  // const zfile = 'x10.zip';
  // const zfile = 'list_person_all_extended_utf8.zip';
  const zfile: null = null;

  const input: string = await get_csv_data(zfile);
  const options: csvlib.Options = {from: 2};
  const parse_result = csvparse(input, options);
  const data = parse_result.map(type_conversion);

  const updated = await db.updated(data, refresh);

  let count = 0;
  if (updated.length > 0) {
    count = await db.import(updated);
  }
  // tslint:disable-next-line:no-console
  console.log(`${count} entries are updated`);
}

async function run() {
  const option_defs: commandLineArgs.OptionDefinition[] = [
    { name: 'refresh', alias: 'r', type: Boolean , defaultValue: false},
    { name: 'backend', alias: 'b', type: String, defaultValue: 'mongo' },
  ];
  const options = commandLineArgs(option_defs);

  const {make_db} = await import('./db_' + options.backend);

  const db = make_db();
  await db.connect();
  const refresh = options.refresh;
  await import_to_db(db, refresh);
  await db.close();
}

run();
