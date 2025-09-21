export default {
  async fetch(request, env) {
    const { pathname } = new URL(request.url);

    if (pathname === "/health") {
      return new Response(
        `ok | base=${env.RESIOT_BASE} | var=${env.RESIOT_VAR_NAME}`,
        { headers: { "content-type": "text/plain" } }
      );
    }

    return new Response("Worker up");
  }
};