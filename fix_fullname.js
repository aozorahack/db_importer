require('dotenv').config();

const mongodb = require('mongodb');
const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

const fix_fullname_books = async (db) => {
  const books = db.collection('books');
  const bulk = books.initializeUnorderedBulkOp();

  const cur = books.find();
  while(true) {
    let book = await cur.next();
    if (book == null) {
      console.log(`${bulk.length} books will be changed`);
      if (bulk.length > 0) {
        let bulk_result = await bulk.execute();
        console.log('done\n', bulk_result.toJSON());
      }
      break;
    }

    let changed = false;
    // console.log(book.book_id);
    ['authors', 'translators', 'editors', 'revisers'].forEach((role) => {
      if (book[role]) {
        book[role].map((person) => {
          if (person.full_name) {
            delete person.full_name;
            changed = true;
          }
        });
      }
    });
    if (changed) {
      bulk.find({_id: book._id}).updateOne(book);
    }
  }
};

const fix_fullname_persons = async (db) => {
  const persons = db.collection('persons');
  const bulk = persons.initializeUnorderedBulkOp();

  const cur = persons.find();
  while(true) {
    let person = await cur.next();
    if (person == null) {
      console.log(`${bulk.length} persons will be changed`);
      if (bulk.length > 0) {
        let bulk_result = await bulk.execute();
        console.log('done\n', bulk_result.toJSON());
      }
      break;
    }

    // console.log(person.person_id);
    if (person.full_name) {
      delete person.full_name;
      bulk.find({_id: person._id}).updateOne(person);
    }
  }
};

const run = async () => {
  const db = await mongodb.MongoClient.connect(mongo_url);
  try {
    const ops = [fix_fullname_books(db),
                 fix_fullname_persons(db)];
    await Promise.all(ops);
  } catch (error) {
    console.error(error);
  }
  db.close();
};

run();
