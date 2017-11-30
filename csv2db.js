const request = require('request-promise');
const AdmZip = require('adm-zip');
const csvparse = require('csv-parse/lib/sync');

require('dotenv').config();

const mongodb = require('mongodb');
const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

const LIST_URL_BASE = 'https://github.com/aozorabunko/aozorabunko/raw/master/index_pages/';
const LISTFILE_INP = 'list_inp_person_all_utf8.zip';
const LIST_URL_PUB = 'list_person_all_extended_utf8.zip';

PERSON_EXTENDED_ATTRS = [
  'book_id',
  'title',
  'title_yomi',
  'title_sort',
  'subtitle',
  'subtitle_yomi',
  'original_title',
  'first_appearance',
  'ndc_code',
  'font_kana_type',
  'copyright',
  'release_date',
  'last_modified',
  'card_url',
  'person_id',
  'last_name',
  'first_name',
  'last_name_yomi',
  'first_name_yomi',
  'last_name_sort',
  'first_name_sort',
  'last_name_roman',
  'first_name_roman',
  'role',
  'date_of_birth',
  'date_of_death',
  'author_copyright',
  'base_book_1',
  'base_book_1_publisher',
  'base_book_1_1st_edition',
  'base_book_1_edition_input',
  'base_book_1_edition_proofing',
  'base_book_1_parent',
  'base_book_1_parent_publisher',
  'base_book_1_parent_1st_edition',
  'base_book_2',
  'base_book_2_publisher',
  'base_book_2_1st_edition',
  'base_book_2_edition_input',
  'base_book_2_edition_proofing',
  'base_book_2_parent',
  'base_book_2_parent_publisher',
  'base_book_2_parent_1st_edition',
  'input',
  'proofing',
  'text_url',
  'text_last_modified',
  'text_encoding',
  'text_charset',
  'text_updated',
  'html_url',
  'html_last_modified',
  'html_encoding',
  'html_charset',
  'html_updated'
];

ROLE_MAP = {
  '著者': 'authors',
  '翻訳者': 'translators',
  '編者': 'editors',
  '校訂者': 'revisers'
};

const get_bookobj = (entry) => {
  let book = {};
  let role = null;
  let person = {};

  // console.log(entry);
  PERSON_EXTENDED_ATTRS.forEach((e,i) => {
    let value = entry[i];
    if(value != '') {
      if(['book_id', 'person_id', 'text_updated', 'html_updated'].includes(e)) {
        value = parseInt(value);
      } else if(['copyright', 'author_copyright'].includes(e)) {
        value = value != 'なし';
      } else if(['release_date', 'last_modified', 'date_of_birth', 'date_of_death',
                 'text_last_modified', 'html_last_modified'].includes(e)) {
        value = new Date(value);
      }

      // console.log(`${e}, ${i}, ${value}`);

      if(['person_id', 'first_name', 'last_name', 'last_name_yomi', 'first_name_yomi',
          'last_name_sort', 'first_name_sort', 'last_name_roman', 'first_name_roman',
          'date_of_birth', 'date_of_death', 'author_copyright'].includes(e)) {
        person[e] = value;
      } else if(e == 'role') {
        role = ROLE_MAP[value];
        if(!role) {
          // console.log(value);
        }
      } else {
        book[e] = value;
      }
    }
  });
  return {book: book, role: role, person: person};
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

const store_books = async (books, books_batch_list) => {
  const books_batch = books.initializeUnorderedBulkOp();
  for (let book_id in books_batch_list) {
    let book = books_batch_list[book_id];
    books_batch.find({book_id: book_id}).upsert().updateOne(book);
  }
  return books_batch.execute();
};

const store_persons = async (persons, persons_batch_list) => {
  const persons_batch = persons.initializeUnorderedBulkOp();
  for (let person_id in persons_batch_list) {
    let person = persons_batch_list[person_id];
    persons_batch.find({person_id: person_id}).upsert().updateOne(person);
  }
  return persons_batch.execute();
};

const import_to_db = async (db, refresh) => {
  refresh = refresh || false;

  const books = db.collection('books');
  const persons = db.collection('persons');

  // const data = csvparse(await get_csv_data('list_person_all_extended_utf8.zip'));
  const data = csvparse(await get_csv_data());

  let updated;
  if (refresh) {
    updated = data;
      
  } else {
    const the_latest_item = await books.findOne({}, {fields: {release_date: 1},
                                                     sort: {release_date: -1}});
    const last_release_date =
          (the_latest_item)? the_latest_item.release_date: new Date('1970-01-01');

    updated = data.slice(1).filter((entry) => {
      return last_release_date < new Date(entry[11]);
    });
  }

  console.log(`${updated.length} entries are updated`);

  if(updated.length > 0) {
    let books_batch_list = {};
    let persons_batch_list = {};
    await Promise.all(updated.map(async (entry) => {
      let {book, role, person} = get_bookobj(entry);
      if (!books_batch_list[book.book_id]) {
        books_batch_list[book.book_id] = book;
      }
      if (!books_batch_list[book.book_id][role]) {
        books_batch_list[book.book_id][role] = [];
      }
      books_batch_list[book.book_id][role].push({
        person_id: person.person_id,
        last_name: person.last_name,
        first_name: person.first_name
      });
      if (!persons_batch_list[person.person_id]) {
        persons_batch_list[person.person_id] = person;
      }
    }));
    await Promise.all([
      store_books(books, books_batch_list),
      store_persons(persons, persons_batch_list)
    ]);
  }
  db.close();
};

const run = async () => {
  
  const db = await mongodb.MongoClient.connect(mongo_url);
  const refresh = process.argv[2];
  import_to_db(db, refresh);
};

run();




