module.exports = {
  apps: [
    {
      name: "server",
      script: "./server.ts",
      interpreter: "node",
      interpreter_args: "--import tsx",
      exec_mode: "fork",
    },
    {
      name: "csv-worker",
      script: "./worker.ts",
      interpreter: "node",
      interpreter_args: "--import tsx",
      instances: 20,
      exec_mode: "cluster",
    },
  ],
};