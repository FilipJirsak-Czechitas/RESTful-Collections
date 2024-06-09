import {Hono} from "@hono/hono";
import {createCollections, RESTfulOptions} from "../mod.ts";

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
});

const app = new Hono();
app.route("/api", collections.buildServer());

export default app;
