/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

export interface Env {
	// Example binding to KV. Learn more at https://developers.cloudflare.com/workers/runtime-apis/kv/
	// MY_KV_NAMESPACE: KVNamespace;
	//
	// Example binding to Durable Object. Learn more at https://developers.cloudflare.com/workers/runtime-apis/durable-objects/
	// MY_DURABLE_OBJECT: DurableObjectNamespace;
	//
	// Example binding to R2. Learn more at https://developers.cloudflare.com/workers/runtime-apis/r2/
	// MY_BUCKET: R2Bucket;
	//
	// Example binding to a Service. Learn more at https://developers.cloudflare.com/workers/runtime-apis/service-bindings/
	// MY_SERVICE: Fetcher;
	//
	// Example binding to a Queue. Learn more at https://developers.cloudflare.com/queues/javascript-apis/
	// MY_QUEUE: Queue;
}

import { XMLParser } from "fast-xml-parser";
import { Kysely, Generated } from 'kysely';
import { D1Dialect } from 'kysely-d1';

export interface Env {
  DB: D1Database;
}

interface EpisodesTable {
  id: Generated<number>,
  title: string,
  link: string,
  pubDate: string,
  createdAt: Generated<number>,
}

interface ShownotesTable {
  id: Generated<number>,
  episodeId: number,
  title: string,
  link: string,
  createdAt: Generated<number>,
}

interface Database {
  episodes: EpisodesTable,
  shownotes: ShownotesTable,
}

const alwaysArray = ['ul', 'ul.li'];

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const response = await (await fetch('https://feeds.rebuild.fm/rebuildfm')).text();
    const options = {
      ignoreAttributes: false,
      attributeNamePrefix: '__',
      textNodeName: '$text',
      isArray: (name, jpath, isLeafNode, isAttribute) => {
        return alwaysArray.indexOf(jpath) !== -1 ? true : false
      }
    }

    const parser = new XMLParser(options);
    const feedObj = parser.parse(response);

    const db = new Kysely<Database>({ dialect: new D1Dialect({ database: env.DB }) });

    for (const elm of feedObj.rss.channel.item) {
      const descObj = parser.parse(elm.description);

      const pubDate = new Date(elm.pubDate);

      const selectResult = await db
        .selectFrom('episodes')
        .select(({ fn, val, ref }) => [
          fn.count<string>('episodes.title').as('title_count')
        ])
        .where('link', '=', elm.link)
        .where('pubDate', '=', pubDate.toISOString())
        .executeTakeFirstOrThrow();

      if (parseInt(selectResult.title_count) > 0) {
        continue;
      }

      const insertResult = await db
        .insertInto('episodes')
        .values({
          title: elm.title,
          link: elm.link,
          pubDate: pubDate.toISOString(),
        })
        .returning(['id'])
        .executeTakeFirstOrThrow();

      for (const ulElm of descObj.ul) {
        for (const liElm of ulElm.li) {
          if (liElm.a) {
            if (liElm.a.$text === undefined) {
              console.log("========================== TODO: これを捨ててる " + elm.title + " " + JSON.stringify(liElm));
              continue;
            }
            await db
              .insertInto('shownotes')
              .values({
                episodeId: insertResult.id,
                title: liElm.a.$text,
                link: liElm.a.__href,
              })
              .executeTakeFirstOrThrow();
          } else {
            if (typeof (liElm) === 'object') {
              console.log("========================== TODO: これを捨ててる " + elm.title + " " + JSON.stringify(liElm));
              continue;
            }
          }
        }
      }
    }
    return new Response("OK");
  },
};
