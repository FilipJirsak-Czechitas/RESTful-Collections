import {Hono} from "@hono/hono";
import {createCollections, RESTfulOptions} from "../mod.ts";
import {Context} from "@hono/hono";

type Task = {
  project: string;
  date: string;
  time?: string;
};

const collections = await createCollections({
  "projects": {},
  "tasks": {
    secondaryIndexes: {
      "by-project": (
        value: Task,
      ): Deno.KvKey => (value.time
        ? [value.project, value.date, value.time]
        : [value.project, value.date, "*"]),
    },
  } satisfies RESTfulOptions<Task>,
  books: {},
  films: {},
  notifications: {
    internal: true,
  },
  checkbox: {},
});

const api = collections.buildServer()
api.get("/notifications", async (c:Context) => c.json(await collections.collections.notifications.list()))

const app = new Hono();
app.route("/api", api);

export default app;
