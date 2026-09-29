import { useState } from "react";
import type { ChangeEvent } from "react";
import axios from 'axios';

function FileUpload() {
  const [file, setFile] = useState<File | null>(null);
  const [email,setEmail] = useState<string>("");
  const [uploading, setUploading] = useState(false);
  const apiUrl = import.meta.env.VITE_API_URL
  const isValidEmail = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);

  function handleFileChange(event: ChangeEvent<HTMLInputElement>) {
    const selectedFile = event.target.files?.[0];
    if (!selectedFile) return;

    if (!selectedFile.name.toLowerCase().endsWith(".csv")) {
      alert("Please select a CSV file.");
      event.target.value = "";
      return;
    }

    setFile(selectedFile);
  }

  function handleRemoveFile() {
    setFile(null);
    setEmail("");
  }

  async function handleSubmit() { 
    if (!file) {
      alert("Please upload a csv file")
      return
    }
    if (!email) {
      alert("Please enter a valid email")
      return
    }
    try { 
      setUploading(true); 
      const formData = new FormData(); 
      formData.append("file", file); 
      formData.append("email", email);
      const response = await axios.post( `${apiUrl}/files/upload`, formData ); 
      handleRemoveFile()
      alert(response?.data?.message); 
    } catch (error) { 
      console.error("Upload failed:", error); 
      alert("File Uploading failed"); 
    } finally { 
      setUploading(false);  
    } 
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100">
      <div className="bg-white p-8 rounded-xl shadow-md w-full max-w-md text-center">
        <h1 className="text-2xl font-semibold mb-2">
          Upload File
        </h1>

        <p className="text-gray-500 mb-6">
          Select a file to upload
        </p>

        <div className="flex flex-col gap-2">
          <label className="inline-block bg-black text-white px-6 py-3 rounded-lg cursor-pointer hover:bg-gray-800">
            Choose File

            <input
              type="file"
              className="hidden"
              onChange={handleFileChange}
            />
          </label>

          <input
            type="email"
            value={email}
            accept=".csv"
            onChange={((e)=>setEmail(e.target.value))}
            placeholder="Enter your email"
            className="bg-white text-black border border-black px-6 py-3 rounded-lg outline-none placeholder-gray-400 focus:ring-2 focus:ring-gray-500"
          />
        </div>

        {file && (
          <div className="mt-5 p-4 bg-gray-50 rounded-lg text-left flex items-center justify-between">
            <div className="min-w-0">
              <p className="font-medium truncate">
                📄 {file.name}
              </p>

              <p className="text-sm text-gray-500 mt-1">
                {(file.size / (1024 * 1024)).toFixed(2)} MB
              </p>
            </div>

            <button
              onClick={handleRemoveFile}
              className="ml-4 text-gray-500 hover:text-red-500 text-xl"
            >
              ✕
            </button>
          </div>
        )}

        <button
          onClick={handleSubmit}
          disabled={!file || !isValidEmail || uploading}
          className={`w-full mt-5 py-3 rounded-lg text-white font-medium ${
            file && isValidEmail && !uploading
              ? "bg-black hover:bg-gray-800 cursor-pointer"
              : "bg-gray-300 cursor-not-allowed"
          }`}
        >
          {uploading ? "Uploading..." : "Submit"}
        </button>
      </div>
    </div>
  );
}

export default FileUpload;
