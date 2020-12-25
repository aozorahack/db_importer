import commandLineArgs from 'command-line-args';

import * as cheerio from 'cheerio';
import * as rp from 'request-promise';

import { IDB } from './i_db';

import * as dotenv from 'dotenv';
dotenv.config();

type Worker = {
    [index: string]: string;

    id: string;
    name: string;
};

function transform(body: string): object[] {
    const $ = cheerio.load(body);
    const items = $('tr[valign]')
        .map(function() {
            return {
                id: parseInt(
                    $(this)
                        .find(':nth-child(1)')
                        .text()
                        .trim(),
                    10
                ),
                name: $(this)
                    .find(':nth-child(2)')
                    .text()
                    .trim()
                    .replace('　', ' ')
            };
        })
        .get()
        .slice(1);

    return items.map((item: Worker) => {
        return { updateOne: { filter: { id: item.id }, update: {$set: item}, upsert: true } };
    });
}

const idurls: { [index: string]: string } = {
    // 'persons': 'http://reception.aozora.gr.jp/pidlist.php?page=1&pagerow=-1',
    workers: 'http://reception.aozora.gr.jp/widlist.php?page=1&pagerow=-1'
};

async function import_to_db(db: IDB): Promise<void> {
    for (const idname of Object.keys(idurls)) {
        const url = idurls[idname];
        const options = { url, transform };
        const bulk_ops = await rp.get(options);
        const result = await db.import_byname(idname, bulk_ops);
        // eslint-disable-next-line no-console
        console.log(`updated ${result} entries`);
    }
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
