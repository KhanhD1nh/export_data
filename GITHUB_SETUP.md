# GitHub Setup Guide

## Step 1: Create a New Repository on GitHub

1. Go to [GitHub](https://github.com) and sign in
2. Click the "+" icon in the top right corner
3. Select "New repository"
4. Fill in the repository details:
   - Repository name: `xml-to-postgresql-extractor` (or your preferred name)
   - Description: "Extract cadastral XML data to PostgreSQL with multithreading support"
   - Visibility: **Public** (as requested)
   - **DO NOT** initialize with README, .gitignore, or license (we already have these)
5. Click "Create repository"

## Step 2: Add Remote and Push

After creating the repository, GitHub will show you the repository URL. Use one of these commands:

### If using HTTPS:

```bash
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
git branch -M main
git push -u origin main
```

### If using SSH:

```bash
git remote add origin git@github.com:YOUR_USERNAME/YOUR_REPO_NAME.git
git branch -M main
git push -u origin main
```

## Step 3: Verify

Visit your repository on GitHub to verify that all files have been pushed successfully.

## Notes

- Replace `YOUR_USERNAME` with your GitHub username
- Replace `YOUR_REPO_NAME` with your repository name
- If you encounter authentication issues, you may need to:
  - Use a Personal Access Token (for HTTPS)
  - Set up SSH keys (for SSH)

