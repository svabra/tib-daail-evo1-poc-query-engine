export const ingestionTypeCatalog = [
  {
    id: "csv",
    label: "CSV Files",
    status: "supported",
    frontendModule: "./csv/controller.js",
    backendModule: "bit_data_workbench.backend.ingestion_types.csv",
  },
  {
    id: "json",
    label: "JSON Files",
    status: "planned",
  },
  {
    id: "parquet",
    label: "Parquet Files",
    status: "planned",
  },
  {
    id: "pipeline",
    label: "Pipeline / Connector",
    status: "planned",
  },
];
