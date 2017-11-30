require('dotenv').config();

const mongodb = require('mongodb');
const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

const fix_fullname = async (db) => {
  const books = db.collection('books');
  let books_batch_list = {};

  book_cur = books.find();

  await book_cur.each((err, book) => {
    if (book == null) {
      db.close();
      return;
    }

    ['authors', 'translators', 'editors', 'revisers'].forEach((role) => {
      if (book[role]) {
        book[role].map((person)=>{delete person.full_name;});
      }
    });
      
    books.update({_id: book._id}, book);
  });
}

const run = async () => {
  const db = await mongodb.MongoClient.connect(mongo_url);
  fix_fullname(db);
};

run();
