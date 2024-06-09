import {type Context, Hono} from "@hono/hono";
import type {StatusCode} from "@hono/hono/utils/http-status";
import {ulid} from "@std/ulid";

/*
type ParamMapper = (c: Context) => string;
*/

export type KeyBuilder<Value> = (value: Value) => Deno.KvKey;
export type KeyBuilders<Value> = { [name: string]: KeyBuilder<Value> };

export type RESTfulGlobalOptions = {
  createId: () => string;
};

export type RESTfulOptionsAll<Value = unknown> = RESTfulGlobalOptions & {
  secondaryIndexes: KeyBuilders<Value>;
};

export type RESTfulOptions<Value = unknown> = Partial<RESTfulOptionsAll<Value>>;

export type ResultWithMetadata<Value> = Value & {
  $$id: string;
  $$versionstamp: string;
};

/*
type ListSelector = {
  prefix: Deno.KvKey;
  start?: Deno.KvKey;
  end?: Deno.KvKey;
};

type ListKeyBuilder = (c: Context) => Deno.KvKey;

type KeyId = {
  key: Deno.KvKey;
  id: string;
};

class DenoKVCollection<Value> {
  constructor(
    private kv: Deno.Kv,
    private name: string,
    private prependKey: KeyPrependBuilder[],
    private keyBuilders?: KeyBuilders<Value>,
  ) {}

  get(getId: ParamMapper) {
    return async (c: Context) => {
      const keyId = this.buildItemKey(c, getId);
      const result = await this.getById(keyId);
      return c.json(this.buildResponse(keyId, result), 200);
    };
  }

  append(createId: ParamMapper) {
    return async (c: Context) => {
      const value: Value = (await c.req.json()) as Value;
      const keyId = this.buildItemKey(c, createId);
      const result = await this.kv.set(keyId.key, value);
      await this.appendSecondaryKeys(c, keyId, value);
      return c.json(this.buildResponse(keyId, result, value), 201);
    };
  }

  replace(getId: ParamMapper) {
    return async (c: Context) => {
      const value: Value = (await c.req.json()) as Value;
      const keyId = this.buildItemKey(c, getId);

      const getResult = await this.getById(keyId);
      const setResult = await this.kv.set(keyId.key, value);
      await this.deleteSecondaryKeys(c, getResult.value!);
      await this.appendSecondaryKeys(c, keyId, value);
      return c.json(this.buildResponse(keyId, setResult, value), 200);
    };
  }

  merge(getId: ParamMapper) {
    return async (c: Context) => {
      //TODO type
      const value: Value = (await c.req.json()) as Value;

      const keyId = this.buildItemKey(c, getId);
      const getResult = await this.getById(keyId);
      const newValue = { ...getResult.value, ...value };
      const setResult = await this.kv.set(keyId.key, newValue);
      await this.deleteSecondaryKeys(c, getResult.value!);
      await this.appendSecondaryKeys(c, keyId, newValue);
      return c.json(this.buildResponse(keyId, setResult, newValue), 200);
    };
  }

  delete(getId: ParamMapper) {
    return async (c: Context) => {
      const keyId = this.buildItemKey(c, getId);

      const result = await this.getById(keyId);
      await this.kv.delete(keyId.key);
      await this.deleteSecondaryKeys(c, result.value!);
      return c.body(null, 204);
    };
  }

  list(keyBuilder?: ListKeyBuilder) {
    return async (c: Context) => {
      const collectionKey = this.buildCollectionKey(c);
      const keyPrep = [];
      if (keyBuilder) {
        //TODO FJ správné vytváření posledního prvku klíče
        keyPrep.push(
          ...this.buildCollectionKey(c, this.name + "$$"),
          ...keyBuilder(c),
        );
      } else {
        keyPrep.push(...collectionKey);
      }
      const key = keyPrep.filter((key) => key !== "");

      const { start, end, limit, reverse } = c.req.query();

      const listSelector: ListSelector = { prefix: key };
      if (start) {
        listSelector.start = key.toSpliced(-1, 0, start);
      } else if (end) {
        listSelector.end = key.toSpliced(-1, 0, end);
      }

      const listOptions: Deno.KvListOptions = {};
      if (limit) {
        listOptions.limit = Number(limit);
      }
      if (reverse) {
        listOptions.reverse = reverse === "true";
      }

      const response: ResultMetadata<Value>[] = [];
      if (keyBuilder) {
        const iter = this.kv.list<string>(listSelector, listOptions);
        for await (const item of iter) {
          const id = item.value;
          const result = await this.kv.get<Value>([...collectionKey, id]);
          response.push(this.buildResponse({ id }, result));
        }
      } else {
        const iter = this.kv.list<Value>(listSelector, listOptions);
        for await (const item of iter) {
          const keyId = { id: item.key[item.key.length - 1] as string };
          response.push(this.buildResponse(keyId, item));
        }
      }

      return c.json(response, 200);
    };
  }

  export() {
    return async (c: Context) => {
      const response = [];
      const iter = this.kv.list({ prefix: [] });
      for await (const item of iter) {
        response.push(item);
      }
      return c.json(response, 200);
    };
  }

  private async getById(keyId: KeyId): Promise<Deno.KvEntryMaybe<Value>> {
    const result = await this.kv.get<Value>(keyId.key);
    if (result.versionstamp === null) {
      throw new HTTPException(404);
    }
    return result;
  }

  private buildResponse(
    keyId: { id: string },
    result: Deno.KvEntryMaybe<Value> | Deno.KvCommitResult,
    value?: Value,
  ): ResultMetadata<Value> {
    if (value === undefined) {
      //@ts-ignore TODO FJ spravit typy
      value = result.value!;
    }
    //@ts-ignore TODO FJ spravit typy
    return {
      $$id: keyId.id,
      $$versionstamp: result.versionstamp!,
      ...value,
    };
  }

  private buildCollectionKey(c: Context, name?: string): Deno.KvKeyPart[] {
    name = name ?? this.name;
    const key: Deno.KvKeyPart[] = this.prependKey.flatMap((builder) =>
      builder(c)
    );
    key.push(name);
    return key;
  }

  private buildItemKey(c: Context, idFactory: ParamMapper): KeyId {
    const key = this.buildCollectionKey(c);
    const id = idFactory(c);
    key.push(id);
    return { key, id };
  }

  private buildSecondaryKeys(c: Context, value: Value): Deno.KvKeyPart[][] {
    if (typeof this.keyBuilders === "function") {
      const collectionKey = this.buildCollectionKey(c, this.name + "$$");
      collectionKey.push(...this.keyBuilders(value, c));
      return [collectionKey];
    } else if (typeof this.keyBuilders === "object") {
      return Object.entries(this.keyBuilders).map(([name, keyBuilder]) => {
        keyBuilder(value, c);
        const collectionKey = this.buildCollectionKey(
          c,
          this.name + "$$" + name,
        );
        collectionKey.push(...keyBuilder(value, c));
        return collectionKey;
      });
    }
    return [];
  }

  private async appendSecondaryKeys(c: Context, keyId: KeyId, value: Value) {
    const secondaryKeys = this.buildSecondaryKeys(c, value);
    const promises = secondaryKeys.map((key) => this.kv.set(key, keyId.id));
    await Promise.all(promises);
  }

  private async deleteSecondaryKeys(c: Context, value: Value) {
    const secondaryKeys = this.buildSecondaryKeys(c, value);
    const promises = secondaryKeys.map((key) => this.kv.delete(key));
    await Promise.all(promises);
  }
}

type CollectionBuilderImpl = (hono: Hono, kv: Deno.Kv) => void;

const collectionBuilder = <Value>(
  hono: Hono,
  kv: Deno.Kv,
  name: string,
  options?: RESTfulOptions<Value>,
) => {
  const paramId = "$$id";
  const paramPath = "$$path";
  const opts: RESTfulOptionsMandatory<Value> = {
    prependKey: [],
    idRegexp: "[0-9A-HJKMNP-TV-Z]{26}",
    exportEndpoint: false,
    createId: () => ulid(),
    ...options,
  };

  const varId = `:${paramId}{${opts.idRegexp}}`;
  const getId = (c: Context) => c.req.param(paramId);
  const getPath = (c: Context) => c.req.param(paramPath).split("/");

  const collection: DenoKVCollection<Value> = new DenoKVCollection(
    kv,
    name,
    opts.prependKey,
    opts.keyBuilder,
  );

  hono.get(`/${name}`, collection.list());
  hono.get(`/${name}/${varId}`, collection.get(getId));
  if (typeof opts.keyBuilder === "function") {
    hono.get(`/${name}/:${paramPath}{.+}`, collection.list(getPath));
  } else if (typeof opts.keyBuilder === "object") {
    //TODO FJ
    throw "Not implemented yet!";
    /*
        for (const [key, builder] of Object.entries(opts.keyBuilder)) {
            this.get(`/${name}/${key}/:$$path{.+}`, collection.list(builder));
        }
        * /
  }
  hono.post(`/${name}`, collection.append(opts.createId));
  hono.put(`/${name}/${varId}`, collection.replace(getId));
  hono.patch(`/${name}/${varId}`, collection.merge(getId));
  hono.delete(`/${name}/${varId}`, collection.delete(getId));
  if (opts.exportEndpoint) {
    hono.get(`/${name}/export`, collection.export());
  }
};

export class RESTfulCollectionsXXX {
  private builders: CollectionBuilderImpl[] = [];

  collection<Value>(
    name: string,
    options?: RESTfulOptions<Value>,
  ): RESTfulCollections {
    this.builders.push((hono: Hono, kv: Deno.Kv) =>
      collectionBuilder(hono, kv, name, options)
    );
    return this;
  }

  async buildServer(): Promise<Hono> {
    const hono = new Hono();
    const kv = await Deno.openKv();
    this.builders.forEach((builder) => builder(hono, kv));
    return hono;
  }
}
*/

//--------------------------------------------------------------------------------------

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
  const collections = Object.entries(collectionDefs).map(createCollection);
  return new RESTfulCollections(kv, collections);
};
