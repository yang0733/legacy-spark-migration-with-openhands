#!/usr/bin/env python3
"""
Script to connect to Databricks workspace and download the legacy ETL job
Uses environment variables for secure credential management
"""

from databricks.sdk import WorkspaceClient
import os

def main():
    # Configure Databricks client using environment variables
    token = os.getenv("DATABRICKS_TOKEN")
    host = os.getenv("DATABRICKS_HOST", "https://your-workspace.cloud.databricks.com")
    
    if not token:
        print("Error: DATABRICKS_TOKEN environment variable not set")
        print("Please set your Databricks token: export DATABRICKS_TOKEN=your_token_here")
        return False
    
    # Initialize the workspace client
    w = WorkspaceClient(
        host=host,
        token=token
    )
    
    # Test connection
    try:
        current_user = w.current_user.me()
        print(f"Successfully connected to Databricks workspace!")
        print(f"Current user: {current_user.user_name}")
        print(f"User ID: {current_user.id}")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return False
    
    # Download the legacy ETL script
    script_path = "/Workspace/Users/your-email/legacy_etl_job.py"
    
    try:
        print(f"Downloading script from: {script_path}")
        
        # Get the file content using export method
        response = w.workspace.download(script_path)
        
        # Read the content from the response
        content = response.read()
        
        # Save to local file
        with open("legacy_etl_job.py", "wb") as f:
            f.write(content)
        
        print("Successfully downloaded legacy_etl_job.py")
        return True
        
    except Exception as e:
        print(f"Failed to download script: {e}")
        # Try alternative method using export
        try:
            print("Trying alternative export method...")
            exported_content = w.workspace.export(script_path)
            
            with open("legacy_etl_job.py", "w", encoding="utf-8") as f:
                f.write(exported_content.content)
            
            print("Successfully downloaded using export method")
            return True
        except Exception as e2:
            print(f"Export method also failed: {e2}")
            return False

if __name__ == "__main__":
    main()