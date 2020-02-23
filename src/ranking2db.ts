import commandLineArgs from 'command-line-args';

import * as cheerio from 'cheerio';
import * as rp from 'request-promise';

import { IDB } from './i_db';

import * as dotenv from 'dotenv';
dotenv.config();

const base_url = 'https://www.aozora.gr.jp/access_ranking/';
const re = /.*card(\d+).html/;

function process_response($: CheerioStatic, year_month: string, book_ids: number[]): any[] {
    return $('tr[valign]')
        .map((idx: number, tr: CheerioElement) => {
            if (idx === 0) {
                return null;
            }

            // const rank = $(tr).find('td:nth-child(1)').text();
            const book_id = parseInt(
                re.exec(
                    $(tr)
                        .find('td:nth-child(2) a')
                        .attr('href')
                )[1],
                10
            );

            if (book_ids.includes(book_id)) {
                return null;
            }

            const query = { year_month, book_id };
            const entry = {
                year_month,
                book_id,
                access: parseInt(
                    $(tr)
                        .find('td:nth-child(4)')
                        .text(),
                    10
                )
            };
            return { updateOne: { filter: query, update: entry, upsert: true } };
        })
        .get();
}

type Entry = {
    [index: string]: string;

    year_month: string;
    type: string;
    url: string;
};

type EtagEntry = {
    [index: string]: string;

    etag: string;
};

function transform_top(body: string): Entry[] {
    const $ = cheerio.load(body);
    return $('a')
        .map((idx, a) => {
            const hrefre = /(\d+)_(\d+)_(.+)\.html/.exec($(a).attr('href'));
            if (hrefre) {
                const entry: Entry = {
                    type: hrefre[3],
                    url: base_url + hrefre.input,
                    year_month: hrefre[1] + '_' + hrefre[2]
                };
                return entry;
            } else {
                return null;
            }
        })
        .get();
}

async function import_to_db(db: IDB): Promise<any[]> {
    const base_options = {
        headers: {
            'User-Agent': 'Mozilla/5.0',
            'If-None-Match': ''
        },
        forever: true,
        pool: {
            maxSockets: 1
        }
    };
    const options = Object.assign(
        {
            transform: transform_top,
            url: base_url
        },
        base_options
    );

    const entries: Entry[] = await rp.get(options);
    const plist = entries.map(async (e: Entry) => {
        const year_month = e.year_month;
        const type = e.type;
        const url = e.url;
        const year_month_type = `${year_month}_${type}`;

        const rp_options: rp.Options = Object.assign(
            {
                resolveWithFullResponse: true,
                url
            },
            base_options
        );
        const ranking = 'ranking_' + type;
        const book_ids = await db.find_bookids(ranking, { year_month });
        const entry = await db.find_one<EtagEntry>('ranking_etags', { year_month_type });
        if (entry) {
            options.headers['If-None-Match'] = entry.etag;
        }

        try {
            const res = await rp.get(rp_options);
            const etag = res.headers.etag;
            const bulk_ops = process_response(cheerio.load(res.body), year_month, book_ids);
            if (bulk_ops.length === 0) {
                return null;
            }
            const result = await db.import_byname(ranking, bulk_ops);

            // eslint-disable-next-line no-console
            console.log(`${year_month}_${type}: updated ${result} entries`);
            await db.create_index(ranking, { year_month: 1, book_id: 1 }, { name: 'index' });

            await db.replace_one(
                'ranking_etags',
                { year_month_type },
                { year_month_type, etag },
                { upsert: true }
            );

            // eslint-disable-next-line no-console
            console.log(`Updated etag: ${year_month}_${type}`);
        } catch (error) {
            if (error.statusCode === 404) {
                console.error(error.response.statusMessage + ': ' + error.options.url);
            } else if (error.statusCode !== 304) {
                console.error(error);
            }
        }
    });
    return Promise.all(plist);
}

async function run(): Promise<void> {
    const option_defs: commandLineArgs.OptionDefinition[] = [
        { name: 'backend', alias: 'b', type: String, defaultValue: 'mongo' }
    ];
    const options = commandLineArgs(option_defs);

    const { make_db } = await import('./db_' + options.backend);

    const db = make_db();
    await db.connect();
    await import_to_db(db);
    await db.close();
}

run();
