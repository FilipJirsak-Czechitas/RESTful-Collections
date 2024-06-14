import {type Context, Hono} from "@hono/hono";
import type {StatusCode} from "@hono/hono/utils/http-status";
import {ulid} from "@std/ulid";

export type KeyBuilder<Value> = (value: Value) => Deno.KvKey;
export type KeyBuilders<Value> = { [name: string]: KeyBuilder<Value> };

export type RESTfulGlobalOptions = {
  createId: () => string;
  internal: boolean;
};

export type RESTfulOptionsAll<Value = unknown> = RESTfulGlobalOptions & {
  secondaryIndexes: KeyBuilders<Value>;
};

export type RESTfulOptions<Value = unknown> = Partial<RESTfulOptionsAll<Value>>;

export type ResultWithMetadata<Value> = Value & {
  $$id: string;
  $$versionstamp: string;
};

export class RESTfulCollection<Value = unknown> {
  constructor(
    private kv: Deno.Kv,
    public name: string,
    private createId: () => string,
    private secondaryIndexBuilders: KeyBuilders<Value>,
  ) {}

  async list(): Promise<ResultWithMetadata<Value>[]> {
    const key = [this.name];
    const response: ResultWithMetadata<Value>[] = [];
    const iter = this.kv.list<Value>({ prefix: key });
    for await (const item of iter) {
      const id = item.key[item.key.length - 1] as string;
      const value: ResultWithMetadata<Value> = {
        $$id: id,
        $$versionstamp: item.versionstamp,
        ...item.value,
      };
      response.push(value);
    }
    return response;
  }

  async get(id: string): Promise<ResultWithMetadata<Value> | null> {
    const key = [this.name, id];
    const kvGetResult = await this.kv.get<Value>(key);
    if (kvGetResult.value == null) {
      return null;
    }
    return {
      $$id: id,
      $$versionstamp: kvGetResult.versionstamp!,
      ...kvGetResult.value,
    };
  }

  async append(value: Value): Promise<ResultWithMetadata<Value>> {
    const id = this.createId();
    const key = [this.name, id];
    const resultKvSet = await this.kv.set(key, value);
    if (!resultKvSet.ok) {
      throw "Cannot append value.";
    }
    await this.appendSecondaryKeys(id, value);
    return { $$id: id, $$versionstamp: resultKvSet.versionstamp, ...value };
  }

  async replace(
    id: string,
    value: Value,
  ): Promise<ResultWithMetadata<Value> | null> {
    const key = [this.name, id];
    const kvGetResult = await this.kv.get<Value>(key);
    const oldValue = kvGetResult.value;
    if (oldValue == null) {
      return null;
    }
    const resultKvSet = await this.kv.set(key, value);
    if (!resultKvSet.ok) {
      throw "Cannot replace value.";
    }
    await this.deleteSecondaryKeys(id, oldValue);
    await this.appendSecondaryKeys(id, value);
    return { $$id: id, $$versionstamp: resultKvSet.versionstamp, ...value };
  }

  async merge(
    id: string,
    mergeValue: Partial<Value>,
  ): Promise<ResultWithMetadata<Value> | null> {
    const key = [this.name, id];
    const kvGetResult = await this.kv.get<Value>(key);
    const oldValue = kvGetResult.value;
    if (oldValue == null) {
      return null;
    }

    const value: Value = { ...kvGetResult.value, ...mergeValue } as Value;
    const resultKvSet = await this.kv.set(key, value);
    if (!resultKvSet.ok) {
      throw "Cannot replace value.";
    }
    await this.deleteSecondaryKeys(id, oldValue);
    await this.appendSecondaryKeys(id, value);
    return { $$id: id, $$versionstamp: resultKvSet.versionstamp, ...value };
  }

  async delete(id: string): Promise<ResultWithMetadata<Value> | null> {
    const key = [this.name, id];
    await this.kv.delete(key);
    const kvGetResult = await this.kv.get<Value>(key);
    const oldValue = kvGetResult.value;
    if (oldValue == null) {
      return null;
    }
    await this.kv.delete(key);
    await this.deleteSecondaryKeys(id, oldValue);
    return {
      $$id: id,
      $$versionstamp: kvGetResult.versionstamp!,
      ...oldValue,
    };
  }

  private buildSecondaryKeys(id: string, value: Value): Deno.KvKey[] {
    return Object.entries(this.secondaryIndexBuilders)
      .map((
        [indexName, keyBuilder],
      ) => [this.name, name, ...keyBuilder(value)]);
  }

  private async appendSecondaryKeys(id: string, value: Value) {
    const promises = this.buildSecondaryKeys(id, value)
      .map((key) => this.kv.set(key, id));
    await Promise.all(promises);
  }

  private async deleteSecondaryKeys(id: string, value: Value) {
    const promises = this.buildSecondaryKeys(id, value)
      .map((key) => this.kv.delete(key));
    await Promise.all(promises);
  }
}

const buildResponse = async <T>(
  c: Context,
  responseValuePromise: Promise<T>,
  status?: StatusCode,
): Promise<Response> => {
  const response = await responseValuePromise;
  if (response === null) {
    return c.body(null, 404);
  }
  return c.json(response, status ?? 200);
};

const createHonoRoutes = (hono: Hono, collection: RESTfulCollection) => {
  hono.get(
    `/${collection.name}`,
    (c: Context) => buildResponse(c, collection.list()),
  );
  hono.get(
    `/${collection.name}/:id`,
    (c: Context) => buildResponse(c, collection.get(c.req.param("id"))),
  );
  hono.post(
    `/${collection.name}`,
    async (c: Context) =>
      buildResponse(c, collection.append(await c.req.json()), 201),
  );
  hono.put(
    `/${collection.name}/:id`,
    async (c: Context) =>
      buildResponse(
        c,
        collection.replace(c.req.param("id"), await c.req.json()),
      ),
  );
  hono.patch(
    `/${collection.name}/:id`,
    async (c: Context) =>
      buildResponse(
        c,
        collection.merge(c.req.param("id"), await c.req.json()),
      ),
  );
  hono.delete(`/${collection.name}/:id`, async (c: Context) => {
    await collection.delete(c.req.param("id"));
    return c.body(null, 204);
  });
};

export class RESTfulCollections {
  constructor(
    public kv: Deno.Kv,
    public collections: RESTfulCollection[],
  ) {}

  buildServer(hono?: Hono) {
    hono ??= new Hono();
    this.collections.forEach((collection) =>
      createHonoRoutes(hono, collection)
    );
    return hono;
  }
}

interface CollectionsConfiguration {
  [key: string]: Partial<RESTfulOptions>;
}
const defaultOptions: RESTfulOptionsAll = {
  createId: () => ulid(),
  //idRegexp: "[0-9A-HJKMNP-TV-Z]{26}",
  secondaryIndexes: {},
  internal: false,
};

export const createCollections = async (
  collectionDefs: CollectionsConfiguration,
  globalOptions?: Partial<RESTfulGlobalOptions>,
): Promise<RESTfulCollections> => {
  const kv = await Deno.openKv();
  const createCollection = <Value>(
    [name, collectionOptions]: [string, RESTfulOptions<Value>],
  ): RESTfulCollection<Value> => {
    const options: RESTfulOptionsAll<Value> = {
      ...defaultOptions,
      ...globalOptions,
      ...collectionOptions,
    };
    return new RESTfulCollection(
      kv,
      name,
      options.createId,
      options.secondaryIndexes,
    );
  };
  const collections = Object.entries(collectionDefs)
      .filter(([_, options]) => !options.internal)
      .map(createCollection);
  return new RESTfulCollections(kv, collections);
};
