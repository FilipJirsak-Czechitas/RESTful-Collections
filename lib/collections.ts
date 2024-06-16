import {type Context, Hono} from "@hono/hono";
import type {StatusCode} from "@hono/hono/utils/http-status";
import {ulid} from "@std/ulid";

/**
 * KeyBuilder is a type that represents a function that takes a collection value and returns a key for secondary index (Deno.KvKey).
 * @typedef {function} KeyBuilder
 * @param {Value} value - The value in collection, which will be converted into a Deno.KvKey.
 * @returns {Deno.KvKey} - The resulting Deno.KvKey (key for secondary index).
 */
export type KeyBuilder<Value> = (value: Value) => Deno.KvKey;

/**
 * KeyBuilders is a type that represents an object where each property is a KeyBuilder function.
 * Property name is name of subcollection.
 * @typedef {Object} KeyBuilders
 */
export type KeyBuilders<Value> = { [name: string]: KeyBuilder<Value> };

/**
 * RESTfulGlobalOptions is a type that represents the global options for a RESTful service.
 * @typedef {Object} RESTfulGlobalOptions
 * @property {function} createId - A function that generates a unique ID of collection item.
 * @property {boolean} internal - A flag indicating whether the collection is internal, i.e. it is not published into REST API.
 */
export type RESTfulGlobalOptions = {
  createId: () => string;
  internal: boolean;
};

/**
 * RESTfulOptionsAll is a type that represents all the options for a RESTful service.
 * @typedef {Object} RESTfulOptionsAll
 * @property {KeyBuilders} secondaryIndexes - An object containing secondary index KeyBuilder functions.
 */
export type RESTfulOptionsAll<Value = unknown> = RESTfulGlobalOptions & {
  secondaryIndexes: KeyBuilders<Value>;
};

/**
 * RESTfulOptions is a type that represents a subset of the options for a RESTful service. All options are optional.
 * @typedef {Object} RESTfulOptions
 */
export type RESTfulOptions<Value = unknown> = Partial<RESTfulOptionsAll<Value>>;

/**
 * ResultWithMetadata is a type that represents a collection item with additional metadata.
 * @typedef {Object} ResultWithMetadata
 * @property {string} $$id - The unique ID of the collection item.
 * @property {string} $$versionstamp - The timestamp of the value (last modification date and time).
 */
export type ResultWithMetadata<Value> = Value & {
  $$id: string;
  $$versionstamp: string;
};

const secondaryIndexSuffix = "$$secondaryIndex";

/**
 * RESTfulCollection is a class that represents a RESTful collection.
 * @class
 * @property {Deno.Kv} kv - The Deno.Kv instance used for key-value storage.
 * @property {string} name - The name of the collection.
 * @property {function} createId - A function that generates a unique ID.
 * @property {KeyBuilders} secondaryIndexBuilders - An object containing secondary index KeyBuilder functions.
 */
export class RESTfulCollection<Value = unknown> {
  constructor(
    private kv: Deno.Kv,
    public name: string,
    public options: RESTfulOptionsAll<Value>
  ) {}

  /**
   * This method lists all items in the collection.
   * @async
   * @method
   * @returns {Promise<ResultWithMetadata<Value>[]>} - A promise that resolves to an array of items in the collection.
   */
  async list(): Promise<ResultWithMetadata<Value>[]> {
    const key = [this.name];
    const response: ResultWithMetadata<Value>[] = [];
    const iter = this.kv.list<Value>({ prefix: key });
    for await (const item of iter) {
      const id = item.key.at(-1) as string;
      const value: ResultWithMetadata<Value> = {
        $$id: id,
        $$versionstamp: item.versionstamp,
        ...item.value,
      };
      response.push(value);
    }
    return response;
  }

  /**
   * This method lists all items in a subcollection.
   * @async
   * @method
   * @param {string} subcollection - The name of the subcollection.
   * @param {string[]} keys - The keys of the items to list.
   * @returns {Promise<ResultWithMetadata<Value>[]>} - A promise that resolves to an array of items in the subcollection.
   */
  async listSubcollection(
    subcollection: string,
    keys: string[],
  ): Promise<ResultWithMetadata<Value>[]> {
    const key = [this.name + secondaryIndexSuffix, subcollection, ...keys];
    const response: ResultWithMetadata<Value>[] = [];
    const iter = this.kv.list<{}>({ prefix: key });
    for await (const item of iter) {
      const id = item.key.at(-1) as string;
      const kvGetResult = await this.kv.get<Value>([this.name, id]);
      if (kvGetResult.value === null) {
        continue;
      }
      const value: ResultWithMetadata<Value> = {
        $$id: id,
        $$versionstamp: kvGetResult.versionstamp!,
        ...kvGetResult.value,
      };
      response.push(value);
    }
    return response;
  }

  /**
   * This method gets an item from the collection by its ID.
   * @async
   * @method
   * @param {string} id - The ID of the item to get.
   * @returns {Promise<ResultWithMetadata<Value> | null>} - A promise that resolves to the item or null if it does not exist.
   */
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

  /**
   * This method appends an item to the collection.
   * @async
   * @method
   * @param {Value} value - The value of the item to append.
   * @returns {Promise<ResultWithMetadata<Value>>} - A promise that resolves to the appended item.
   */
  async append(value: Value): Promise<ResultWithMetadata<Value>> {
    const id = this.options.createId();
    const key = [this.name, id];
    const resultKvSet = await this.kv.set(key, value);
    if (!resultKvSet.ok) {
      throw "Cannot append value.";
    }
    await this.appendSecondaryKeys(id, value);
    return { $$id: id, $$versionstamp: resultKvSet.versionstamp, ...value };
  }

  /**
   * This method replaces an item in the collection by its ID.
   * @async
   * @method
   * @param {string} id - The ID of the item to replace.
   * @param {Value} value - The new value of the item.
   * @returns {Promise<ResultWithMetadata<Value> | null>} - A promise that resolves to the replaced item or null if it does not exist.
   */
  async replace(
    id: string,
    value: Value,
  ): Promise<ResultWithMetadata<Value> | null> {
    const key = [this.name, id];
    const kvGetResult = await this.kv.get<Value>(key);
    const oldValue = kvGetResult.value;
    if (oldValue === null) {
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

  /**
   * This method merges an item in the collection with a partial value by its ID.
   * @async
   * @method
   * @param {string} id - The ID of the item to merge.
   * @param {Partial<Value>} mergeValue - The partial value to merge with the item.
   * @returns {Promise<ResultWithMetadata<Value> | null>} - A promise that resolves to the merged item or null if it does not exist.
   */
  async merge(
    id: string,
    mergeValue: Partial<Value>,
  ): Promise<ResultWithMetadata<Value> | null> {
    const key = [this.name, id];
    const kvGetResult = await this.kv.get<Value>(key);
    const oldValue = kvGetResult.value;
    if (oldValue === null) {
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

  /**
   * This method deletes an item from the collection by its ID.
   * @async
   * @method
   * @param {string} id - The ID of the item to delete.
   * @returns {Promise<ResultWithMetadata<Value> | null>} - A promise that resolves to the deleted item or null if it does not exist.
   */
  async delete(id: string): Promise<ResultWithMetadata<Value> | null> {
    const key = [this.name, id];
    const kvGetResult = await this.kv.get<Value>(key);
    const oldValue = kvGetResult.value;
    if (oldValue === null) {
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
    return Object.entries(this.options.secondaryIndexes)
      .map((
        [indexName, keyBuilder],
      ) => [
        this.name + secondaryIndexSuffix,
        indexName,
        ...keyBuilder(value),
        id,
      ]);
  }

  private async appendSecondaryKeys(id: string, value: Value) {
    const promises = this.buildSecondaryKeys(id, value)
      .map((key) => this.kv.set(key, {}));
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

const buildEmptyResponse = async <T>(
  c: Context,
  responseValuePromise: Promise<T>,
  status?: StatusCode,
): Promise<Response> => {
  const response = await responseValuePromise;

  if (response === null) {
    return c.body(null, 404);
  }
  return c.body(null, status ?? 204);
};

const createHonoRoutes = (hono: Hono, collection: RESTfulCollection) => {
  hono.get(
    `/${collection.name}`,
    (c: Context) => buildResponse(c, collection.list()),
  );
  Object.keys(collection.options.secondaryIndexes)
    .forEach((subCollectionName) => {
      hono.get(
        `/${collection.name}/:${subCollectionName}/:key{.+$}`,
        (c: Context) =>
          buildResponse(
            c,
            collection.listSubcollection(
              subCollectionName,
              c.req.param("key").split("/").filter((part) => part.length > 0),
            ),
          ),
      );
    });
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
  hono.delete(`/${collection.name}/:id`, (c: Context) =>
    buildEmptyResponse(
      c,
      collection.delete(c.req.param("id")),
    ));
};

/**
 * RESTfulCollections is a class that represents a collection of RESTful collections.
 * @class
 * @property {Deno.Kv} kv - The Deno.Kv instance used for key-value storage.
 * @property {Object} collections - An object where each property is a RESTfulCollection.
 */
export class RESTfulCollections {
  /**
   * A public property of the RESTfulCollections class.
   * It is an object where each property is a RESTfulCollection.
   * The property name is the name of the collection.
   * @type {Object.<string, RESTfulCollection>}
   */
  public collections: { [name: string]: RESTfulCollection };

  constructor(
    public kv: Deno.Kv,
    collections: RESTfulCollection[],
  ) {
    this.collections = {};
    collections.forEach((collection) =>
      this.collections[collection.name] = collection
    );
  }

  /**
   * This method builds a Hono server with routes for each collection in the RESTfulCollections instance.
   * If a Hono instance is not provided, a new one is created.
   *
   * @method
   * @param {Hono} [hono] - An optional Hono instance to which the routes will be added.
   * @returns {Hono} - A Hono instance with routes for each collection.
   */
  buildServer(hono?: Hono): Hono {
    hono ??= new Hono();
    Object.values(this.collections)
        .filter(collection => !collection.options.internal)
        .forEach((collection) => createHonoRoutes(hono, collection));
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

/**
 * createCollections is a function that creates a RESTfulCollections instance.
 * @function
 * @param {CollectionsConfiguration} collectionDefs - The definitions for the collections.
 * @param {RESTfulGlobalOptions} globalOptions - The global options for the collections.
 * @returns {Promise<RESTfulCollections>} - A promise that resolves to a RESTfulCollections instance.
 */
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
      options
    );
  };
  const collections = Object.entries(collectionDefs)
    .map(createCollection);
  return new RESTfulCollections(kv, collections);
};
