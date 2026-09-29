import mongoose from "mongoose";
import ExcelJS from "exceljs";
import nodemailer from "nodemailer";
import path from "path";

export async function generateExcelAndEmail(jobId: string, recipientEmail: string) {
  try {
    // 1. Fetch all documents from the dynamic MongoDB collection
    const collection = mongoose.connection.collection(`job_${jobId}`);
    const rows = await collection.find({}).toArray();

    if (rows.length === 0) {
      console.log("No rows found to export.");
      return;
    }

    // 2. Create Excel workbook and worksheet
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet("Processed Data");

    // Dynamic headers based on the keys of the first row
    // (excluding MongoDB's internal _id and rowNumber if desired)
    const firstRowKeys = Object.keys(rows[0]).filter((key) => key !== "_id");
    worksheet.columns = firstRowKeys.map((key) => ({
      header: key,
      key: key,
      width: 20,
    }));

    // Add rows
    rows.forEach((row) => {
      worksheet.addRow(row);
    });

    const exportPath = path.join("./uploads", `export-${jobId}.xlsx`);
    await workbook.xlsx.writeFile(exportPath);
    console.log(`Excel file created at ${exportPath}`);

    // 3. Setup Nodemailer Transporter (example using Gmail or generic SMTP)
    const transporter = nodemailer.createTransport({
      service: "gmail",
      auth: {
        user: process.env.EMAIL_USER, 
        pass: process.env.EMAIL_PASS, 
      },
    });

    // 4. Send Email with the Excel attachment
    await transporter.sendMail({
      from: `"CSV Data Forger" <${process.env.EMAIL_USER}>`,
      to: recipientEmail,
      subject: `Your Processed File is Ready (Job: ${jobId})`,
      text: `Hello,\n\nYour CSV processing job ${jobId} has completed successfully. Please find the attached Excel report.\n\nBest regards,\nCSV Data Forger Team`,
      attachments: [
        {
          filename: `report-${jobId}.xlsx`,
          path: exportPath,
        },
      ],
    });

    console.log(`Email successfully sent to ${recipientEmail}`);

    // 5. Cleanup temporary excel file
    // fs.unlinkSync(exportPath);
  } catch (error) {
    console.error("Error in generateExcelAndEmail:", error);
  }
}