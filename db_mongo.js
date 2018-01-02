require('dotenv').config();

const mongodb = require('mongodb');
const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

const ATTRS = require('./attrs').ATTRS;

//
// utilities
//
const _get_bookobj = (entry) => {
  let book = {};
  let person = {};

  // console.log(entry);
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

class DB {
  async connect() {
    this.db = await mongodb.MongoClient.connect(mongo_url);
  }

  async updated(data) {
    const books = this.db.collection('books');
    const the_latest_item = await books.findOne({}, {fields: {release_date: 1},
                                                     sort: {release_date: -1}});
    const last_release_date =
          (the_latest_item)? the_latest_item.release_date: new Date('1970-01-01');

    return data.slice(1).filter((entry) => {
      return last_release_date < new Date(entry[11]);
    });
  }

  async _store_books(books_batch_list) {
    const books = this.db.collection('books');
    const bulk_ops = Object.keys(books_batch_list).map((book_id) => {
      const book = books_batch_list[book_id];
      return {updateOne: {filter: {book_id: book.book_id},
                          update: book,
                          upsert: true}};
    });
    return books.bulkWrite(bulk_ops, {orderd: false});
  }

  async _store_persons(persons_batch_list) {
    const persons = this.db.collection('persons');
    const bulk_ops = Object.keys(persons_batch_list).map((person_id) => {
      const person = persons_batch_list[person_id];
      return {updateOne: {filter: {person_id: person.person_id},
                          update: person,
                          upsert: true}};
    });
    return persons.bulkWrite(bulk_ops, {orderd: false});
  }

  async import(updated) {
    let books_batch_list = {};
    let persons_batch_list = {};
    await Promise.all(updated.map(async (entry) => {
      const {book, role, person} = _get_bookobj(entry);
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

    return Promise.all([
      this._store_books(books_batch_list),
      this._store_persons(persons_batch_list)
    ]).then((res)=> {
      // console.log(res[0].toJSON());
      return res[0].nUpserted + res[0].nModified;
    });
  }
  async close() {
    this.db.close();
  }
}

exports.DB = DB;
