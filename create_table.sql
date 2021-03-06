CREATE TABLE list (book_id integer,
                   title varchar(128),
                   title_yomi varchar(128),
                   title_sort varchar(128),
                   subtitle varchar(128),
                   subtitle_yomi varchar(128),
                   original_title varchar(256),
                   first_appearance varchar(16384),
                   ndc_code varchar(32),
                   font_kana_type varchar(8),
                   copyright boolean,
                   release_date timestamp without time zone,
                   last_modified timestamp without time zone,
                   card_url varchar(64),
                   person_id integer,
                   last_name varchar(32),
                   first_name varchar(32),
                   last_name_yomi varchar(32),
                   first_name_yomi varchar(32),
                   last_name_sort varchar(32),
                   first_name_sort varchar(32),
                   last_name_roman varchar(32),
                   first_name_roman varchar(32),
                   role varchar(32),
                   date_of_birth varchar(16),
                   date_of_death varchar(16),
                   author_copyright boolean,
                   base_book_1 varchar(256),
                   base_book_1_publisher varchar(256),
                   base_book_1_1st_edition varchar(256),
                   base_book_1_edition_input varchar(256),
                   base_book_1_edition_proofing varchar(256),
                   base_book_1_parent varchar(256),
                   base_book_1_parent_publisher varchar(256),
                   base_book_1_parent_1st_edition varchar(256),
                   base_book_2 varchar(256),
                   base_book_2_publisher varchar(256),
                   base_book_2_1st_edition varchar(256),
                   base_book_2_edition_input varchar(256),
                   base_book_2_edition_proofing varchar(256),
                   base_book_2_parent varchar(256),
                   base_book_2_parent_publisher varchar(256),
                   base_book_2_parent_1st_edition varchar(256),
                   input varchar(32),
                   proofing varchar(32),
                   text_url varchar(128),
                   text_last_modified timestamp without time zone,
                   text_encoding varchar(8),
                   text_charset varchar(16),
                   text_updated integer,
                   html_url varchar(128),
                   html_last_modified timestamp without time zone,
                   html_encoding varchar(8),
                   html_charset varchar(16),
                   html_updated integer,
                   CONSTRAINT id_ukey UNIQUE(book_id, person_id))
