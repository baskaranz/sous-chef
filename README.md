# Sous Chef

A simplified feature store management system built on top of Feast.

## Setup

# Option 1: If repository is public
pip install git+https://github.com/your-org/sous-chef.git

# Option 2: If repository is private
pip install git+https://username:personal_access_token@github.com/your-org/sous-chef.git

# Option 3: Clone and install locally
git clone https://github.com/your-org/sous-chef.git
cd sous-chef
pip install -e .

## Usage

# Create project structure
mkdir your_project
cd your_project
mkdir -p feature_repo/data

Then create configs `feature_views.yaml`, `feature_store.yaml` and use:

from sous_chef import SousChef
chef = SousChef(".")  # Points to directory containing feature_repo
