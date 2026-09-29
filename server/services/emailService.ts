import mongoose from "mongoose";
import ExcelJS from "exceljs";
import nodemailer from "nodemailer";
import path from "path";
import fs from "fs";
import crypto from "crypto";
import { FileExport } from "../models/FileExports";

export async function generateExcelAndEmail(jobId: string, recipientEmail: string) {
  try {
    const exportsDir = path.resolve("./uploads/exports");
    if (!fs.existsSync(exportsDir)) {
      fs.mkdirSync(exportsDir, { recursive: true });
    }

    const fileName = `export-${jobId}.xlsx`;
    const exportPath = path.join(exportsDir, fileName);

    const collection = mongoose.connection.collection(`job_${jobId}`);

    // Peek first document to dynamically extract headers
    const sampleDoc = await collection.findOne({});
    if (!sampleDoc) {
      console.log(`No rows found to export for job_${jobId}.`);
      return;
    }

    // 1. Memory-efficient Excel streaming writer
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
      filename: exportPath,
      useStyles: false,
      useSharedStrings: false,
    });

    const worksheet = workbook.addWorksheet("Processed Data");

    // Dynamic headers (excluding internal Mongo _id)
    const headers = Object.keys(sampleDoc).filter((k) => k !== "_id");
    worksheet.columns = headers.map((header) => ({ header, key: header, width: 20 }));

    // 2. Stream documents from MongoDB cursor (avoids memory overload)
    const cursor = collection.find({}).project({ _id: 0 });
    for await (const doc of cursor) {
      worksheet.addRow(doc).commit();
    }

    worksheet.commit();
    await workbook.commit();
    console.log(`Excel file generated at: ${exportPath}`);

    // 3. Generate 64-char unguessable download token and save metadata
    const downloadToken = crypto.randomBytes(32).toString("hex");
    await FileExport.create({
      jobId,
      email: recipientEmail,
      fileName,
      filePath: exportPath,
      downloadToken,
    });

    // 4. Build download link
    const serverUrl = process.env.SERVER_URL || `http://localhost:${process.env.PORT || 1305}`;
    const downloadUrl = `${serverUrl}/api/files/download/${downloadToken}`;

    // 5. Send email with download button
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: process.env.EMAIL_USER,
        pass: process.env.EMAIL_PASS,
      },
    });

    await transporter.sendMail({
      from: `"CSV Data Forger" <${process.env.EMAIL_USER}>`,
      to: recipientEmail,
      subject: `Your Processed File is Ready (Job: ${jobId})`,
      html: `
        <div style="font-family: Arial, sans-serif; padding: 20px; color: #1f2937;">
          <h2 style="color: #111827;">Your file has been processed!</h2>
          <p>Your CSV processing job <strong>${jobId}</strong> has completed successfully.</p>
          <p style="margin: 24px 0;">
            <a href="${downloadUrl}" style="background-color: #2563eb; color: #ffffff; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: bold; display: inline-block;">
              Download Excel File
            </a>
          </p>
          <p style="color: #6b7280; font-size: 13px;">
            <strong>Important:</strong> For security and storage purposes, this download link will expire and the file will be deleted automatically after <strong>24 hours</strong>.
          </p>
        </div>
      `,
    });

    console.log(`Notification email sent to ${recipientEmail}`);
  } catch (error) {
    console.error("Error in generateExcelAndEmail:", error);
  }
}