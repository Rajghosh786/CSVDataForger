import fs from 'fs';
import type { Request, Response } from "express";
import crypto from "crypto";
import { sendCsvJob } from "../kafka/producer";
import { FileExport } from "../models/FileExports";

export async function uploadFile(req: Request, res: Response) {
  if (!req.file) {
    return res.status(400).json({
      message: "No file uploaded",
    });
  }

  const jobId = crypto.randomUUID();
  const userEmail = req.body.email;

  try {
    await sendCsvJob({
      jobId,
      fileName: req.file.originalname,
      filePath: req.file.path,
      size: req.file.size,
      userEmail
    });

    return res.status(202).json({
      message: "File uploaded and queued for processing",
      jobId,
      file: {
        originalName: req.file.originalname,
        filename: req.file.filename,
        size: req.file.size,
        path: req.file.path,
      },
    });
  } catch (error) {
    console.error("Failed to queue file in Kafka:", error);
    return res.status(500).json({
      message: "Failed to queue file for processing",
    });
  }
}

export async function exportFile(req: Request, res: Response) {
  try {
    const { token } = req.params;

    const record = await FileExport.findOne({ downloadToken: token });

    if (!record || !fs.existsSync(record.filePath)) {
      return res.status(404).send(`
        <div style="font-family: Arial, sans-serif; text-align: center; margin-top: 60px;">
          <h2>Download Link Expired or Not Found</h2>
          <p>This file is no longer available. Links expire automatically after 24 hours.</p>
        </div>
      `);
    }

    res.setHeader("Content-Disposition", `attachment; filename="${record.fileName}"`);
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");

    // Stream the 1 GB file straight to client
    const fileStream = fs.createReadStream(record.filePath);

    fileStream.on("error", (err) => {
      console.error("Error streaming file:", err);
      if (!res.headersSent) {
        res.status(500).send("Error downloading file.");
      }
    });

    fileStream.pipe(res);
  } catch (error) {
    console.error("Download controller error:", error);
    res.status(500).send("Internal server error.");
  }
}