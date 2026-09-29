import mongoose, { Document, Schema } from "mongoose";

export interface IFileExport extends Document {
  jobId: string;
  email: string;
  fileName: string;
  filePath: string;
  downloadToken: string;
  createdAt: Date;
}

const fileExportSchema = new Schema<IFileExport>({
  jobId: { type: String, required: true },
  email: { type: String, required: true },
  fileName: { type: String, required: true },
  filePath: { type: String, required: true },
  downloadToken: { type: String, required: true, unique: true },
  createdAt: {
    type: Date,
    default: Date.now,
    expires: 86400, // automatically drops document after 24 hours
    //expires: 360,
  },
});

fileExportSchema.index(
  { createdAt: 1 },
  { expireAfterSeconds: 60 }
);

export const FileExport = mongoose.model<IFileExport>(
  "FileExport", 
  fileExportSchema
);