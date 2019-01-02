import * as dotenv from 'dotenv';
dotenv.config();

import * as mongodb from 'mongodb';

const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongodb_replica_set = process.env.AOZORA_MONGODB_REPLICA_SET;
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

import {ATTRS} from './attrs';

//
// utilities
//
type StringMap = { [key: string]: string };

type Book = {
  [index: string]: string | StringMap[]

  book_id?: string;
  title?: string;
  title_yomi?: string;
  title_sort?: string;
  subtitle?: string;
  subtitle_yomi?: string;
  original_title?: string;
  first_appearance?: string;
  ndc_code?: string;
  font_kana_type?: string;
  copyright?: string;
  release_date?: string;
  last_modified?: string;
  card_url?: string;
  revisers?: StringMap[];
  editors?: StringMap[];
  translators?: StringMap[];
  authors?: StringMap[];
  base_book_1?: string;
  base_book_1_publisher?: string;
  base_book_1_1st_edition?: string;
  base_book_1_edition_input?: string;
  base_book_1_edition_proofing?: string;
  base_book_1_parent?: string;
  base_book_1_parent_publisher?: string;
  base_book_1_parent_1st_edition?: string;
  base_book_2?: string;
  base_book_2_publisher?: string;
  base_book_2_1st_edition?: string;
  base_book_2_edition_input?: string;
  base_book_2_edition_proofing?: string;
  base_book_2_parent?: string;
  base_book_2_parent_publisher?: string;
  base_book_2_parent_1st_edition?: string;
  input?: string;
  proofing?: string;
  text_url?: string;
  text_last_modified?: string;
  text_encoding?: string;
  text_charset?: string;
  text_updated?: string;
  html_url?: string;
  html_last_modified?: string;
  html_encoding?: string;
  html_charset?: string;
  html_updated?: string;
}

type Person = { [index: string]: string
  person_id?: string;
  first_name?: string;
  last_name?: string;
  last_name_yomi?: string;
  first_name_yomi?: string;
  last_name_sort?: string;
  first_name_sort?: string;
  last_name_roman?: string;
  first_name_roman?: string;
  date_of_birth?: string;
  date_of_death?: string;
  author_copyright?: string;
};

type BookRolePerson = {
  book: Book
  role: string,
  person: Person,
};

type BookList = {[key: string]: Book}
type PersonList = {[key: string]: Person}

function _separate_obj(entry: StringMap): BookRolePerson {
  let book: Book = {};
  let person: Person = {};

  ATTRS.forEach((e,i) => {
    const value = entry[i];

    if(['person_id', 'first_name', 'last_name', 'last_name_yomi', 'first_name_yomi',
        'last_name_sort', 'first_name_sort', 'last_name_roman', 'first_name_roman',
        'date_of_birth', 'date_of_death', 'author_copyright'].includes(e)) {
      person[e] = value;
    } else if(e == 'role') {
      // do nothing
    } else {
      book[e] = value;
    }
  });
  return {book: book, role: entry[ATTRS.indexOf('role')], person: person};
};

import {IDB} from './i_db';

class DB implements IDB {
  client: mongodb.MongoClient;

  constructor() {
  }

  async connect() {
    let options: mongodb.MongoClientOptions = {useNewUrlParser: true};
    if(mongodb_replica_set) {
      options.ssl = true;
      options.replicaSet = mongodb_replica_set;
      options.authMechanism = 'SCRAM-SHA-1';
      options.authSource = 'admin';
    }
    this.client = await mongodb.MongoClient.connect(mongo_url, options);
  }

  collection(name: string) {
    return this.client.db().collection(name);
  }

  async updated(data: string[], refresh: boolean) {
    if (refresh) {
      return data;
    }

    const books = this.collection('books');
    const the_latest_item = await books.findOne({}, {projection: {release_date: 1},
                                                     sort: {release_date: -1}});
    const last_release_date =
          (the_latest_item)? the_latest_item.release_date: new Date('1970-01-01');

    return data.slice(1).filter((entry) => {
      return last_release_date < new Date(entry[11]);
    });
  }

  async _store_books(books_batch_list: BookList) {
    const books = this.client.db().collection('books');
    const operations = Object.keys(books_batch_list).map((book_id) => {
      const book = books_batch_list[book_id];
      return {updateOne: {filter: {book_id: book.book_id},
                          update: book,
                          upsert: true}};
    });
    const options: mongodb.CollectionBulkWriteOptions = { ordered: false };
    return books.bulkWrite(operations, options);
  }

  async _store_persons(persons_batch_list: PersonList) {
    const persons = this.client.db().collection('persons');
    const operations = Object.keys(persons_batch_list).map((person_id) => {
      const person = persons_batch_list[person_id];
      return {updateOne: {filter: {person_id: person.person_id},
                          update: person,
                          upsert: true}};
    });
    const options: mongodb.CollectionBulkWriteOptions = { ordered: false };
    return persons.bulkWrite(operations, options);
  }

  async import(updated: any): Promise<number> {
    let books_batch_list: BookList = {};
    let persons_batch_list: PersonList = {};
    await Promise.all(updated.map((entry: StringMap) => {
      const {book, role, person} = _separate_obj(entry);
      if (!books_batch_list[book.book_id]) {
        // book.persons = [];
        books_batch_list[book.book_id] = book;
      }
      if (!books_batch_list[book.book_id][role]) {
        books_batch_list[book.book_id][role] = [];
      }
      (books_batch_list[book.book_id][role] as StringMap[]).push({
        person_id: person.person_id,
        last_name: person.last_name,
        first_name: person.first_name
      });
      /*
      books_batch_list[book.book_id].persons.push({
        person_id: person.person_id,
        last_name: person.last_name,
        first_name: person.first_name,
        role: role
      });
      */
      if (!persons_batch_list[person.person_id]) {
        persons_batch_list[person.person_id] = person;
      }
    }));

    return Promise.all([
      this._store_books(books_batch_list),
      this._store_persons(persons_batch_list)
    ]).then((res)=> {
      return res[0].upsertedCount + res[0].modifiedCount;
    });
  }
  async close() {
    this.client.close();
  }
}

export function make_db(): IDB {
  return new DB();
}
