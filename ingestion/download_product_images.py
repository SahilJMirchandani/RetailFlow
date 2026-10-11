
import hashlib
import json
import mimetypes
import sys
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


PROJECT_ROOT = Path(__file__).resolve().parent.parent

PRODUCTS_FILE = (
    PROJECT_ROOT / "data" / "silver" / "products" / "products.parquet"
)

IMAGE_DIR = PROJECT_ROOT / "data" / "catalog_images" / "dummyjson"

MANIFEST_FILE = (
    PROJECT_ROOT / "data" / "output" / "product_images_manifest.csv"
)

MAX_IMAGE_BYTES = 8 * 1024 * 1024
MAX_TOTAL_BYTES = 150 * 1024 * 1024
CHUNK_SIZE = 64 * 1024

ALLOWED_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".webp", ".gif"
}

CONTENT_TYPE_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


def create_session():
    """Create an HTTP session with retries for temporary failures."""

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": "RetailFlow-DataEngineering/2.0"
    })

    return session


def get_extension(url, content_type=None):
    """Determine a safe image extension."""

    suffix = Path(urlparse(url).path).suffix.lower()

    if suffix in ALLOWED_EXTENSIONS:
        return suffix

    if content_type in CONTENT_TYPE_EXTENSIONS:
        return CONTENT_TYPE_EXTENSIONS[content_type]

    guessed, _ = mimetypes.guess_type(url)

    if guessed in CONTENT_TYPE_EXTENSIONS:
        return CONTENT_TYPE_EXTENSIONS[guessed]

    return None


def download_one(session, url, remaining_budget):
    """Download one image safely, avoiding duplicate downloads."""

    url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()[:20]
    initial_extension = get_extension(url)

    # Most catalogue image URLs already include a file extension.
    if initial_extension:
        initial_path = IMAGE_DIR / f"{url_hash}{initial_extension}"

        if initial_path.exists() and initial_path.stat().st_size > 0:
            return {
                "status": "already_exists",
                "local_path": initial_path.relative_to(PROJECT_ROOT).as_posix(),
                "file_size_bytes": initial_path.stat().st_size,
                "error": "",
                "new_bytes": 0,
            }

    if remaining_budget <= 0:
        return {
            "status": "skipped_size_limit",
            "local_path": "",
            "file_size_bytes": 0,
            "error": "Total download budget reached.",
            "new_bytes": 0,
        }

    if urlparse(url).scheme != "https":
        return {
            "status": "failed",
            "local_path": "",
            "file_size_bytes": 0,
            "error": "Only HTTPS image URLs are allowed.",
            "new_bytes": 0,
        }

    temp_path = IMAGE_DIR / f"{url_hash}.part"

    try:
        with session.get(
            url,
            timeout=(10, 30),
            stream=True,
        ) as response:
            response.raise_for_status()

            content_type = (
                response.headers.get("Content-Type", "")
                .split(";")[0]
                .strip()
                .lower()
            )

            if not content_type.startswith("image/"):
                raise ValueError(
                    f"Not an image response: {content_type or 'unknown'}"
                )

            extension = get_extension(url, content_type)

            if not extension:
                raise ValueError(
                    f"Unsupported image type: {content_type}"
                )

            destination = IMAGE_DIR / f"{url_hash}{extension}"

            if destination.exists() and destination.stat().st_size > 0:
                return {
                    "status": "already_exists",
                    "local_path": destination.relative_to(
                        PROJECT_ROOT
                    ).as_posix(),
                    "file_size_bytes": destination.stat().st_size,
                    "error": "",
                    "new_bytes": 0,
                }

            content_length = response.headers.get("Content-Length")

            if content_length:
                size = int(content_length)

                if size > MAX_IMAGE_BYTES:
                    raise ValueError("Image exceeds the per-file size limit.")

                if size > remaining_budget:
                    raise ValueError("Image exceeds the remaining download budget.")

            bytes_written = 0

            try:
                with temp_path.open("wb") as output:
                    for chunk in response.iter_content(
                        chunk_size=CHUNK_SIZE
                    ):
                        if not chunk:
                            continue

                        next_size = bytes_written + len(chunk)

                        if next_size > MAX_IMAGE_BYTES:
                            raise ValueError(
                                "Image exceeds the per-file size limit."
                            )

                        if next_size > remaining_budget:
                            raise ValueError(
                                "Image exceeds the remaining download budget."
                            )

                        output.write(chunk)
                        bytes_written = next_size

                if bytes_written == 0:
                    raise ValueError("Downloaded image is empty.")

                temp_path.replace(destination)

            except Exception:
                temp_path.unlink(missing_ok=True)
                raise

        return {
            "status": "downloaded",
            "local_path": destination.relative_to(
                PROJECT_ROOT
            ).as_posix(),
            "file_size_bytes": bytes_written,
            "error": "",
            "new_bytes": bytes_written,
        }

    except (requests.RequestException, OSError, ValueError) as error:
        temp_path.unlink(missing_ok=True)

        return {
            "status": "failed",
            "local_path": "",
            "file_size_bytes": 0,
            "error": str(error),
            "new_bytes": 0,
        }


def download_product_images():
    """Download catalogue images and create a product-image manifest."""

    print("=" * 65)
    print("RetailFlow 2.0 - Product Image Ingestion")
    print("=" * 65)

    if not PRODUCTS_FILE.exists():
        raise FileNotFoundError(
            f"Product catalogue not found: {PRODUCTS_FILE}\n"
            "Run ingestion/ingest_product_catalog.py first."
        )

    IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(PRODUCTS_FILE)

    associations = []

    # Preserve the relationship between every product and each image.
    for _, product in df.iterrows():
        thumbnail = product.get("thumbnail_url")

        try:
            images = json.loads(product["image_urls_json"])
        except (TypeError, ValueError, KeyError):
            images = []

        if not isinstance(images, list):
            images = []

        urls = []

        if isinstance(thumbnail, str) and thumbnail.strip():
            urls.append(thumbnail.strip())

        urls.extend(
            url.strip()
            for url in images
            if isinstance(url, str) and url.strip()
        )

        # Remove duplicate URLs within this product.
        unique_product_urls = list(dict.fromkeys(urls))

        for url in unique_product_urls:
            associations.append({
                "source_system": "dummyjson",
                "source_product_id": product["source_product_id"],
                "title": product["title"],
                "image_url": url,
                "is_thumbnail": url == thumbnail,
            })

    if not associations:
        raise ValueError("No image URLs were found in the catalogue.")

    manifest = pd.DataFrame(associations)

    # Download each distinct image URL only once.
    unique_urls = manifest["image_url"].drop_duplicates().tolist()
    download_results = {}

    downloaded_bytes = 0

    with create_session() as session:
        for index, url in enumerate(unique_urls, start=1):
            result = download_one(
                session,
                url,
                MAX_TOTAL_BYTES - downloaded_bytes,
            )

            downloaded_bytes += result["new_bytes"]
            download_results[url] = result

            if index % 25 == 0 or index == len(unique_urls):
                print(
                    f"Processed {index}/{len(unique_urls)} "
                    f"unique images..."
                )

    manifest["download_status"] = manifest["image_url"].map(
        lambda url: download_results[url]["status"]
    )

    manifest["local_path"] = manifest["image_url"].map(
        lambda url: download_results[url]["local_path"]
    )

    manifest["file_size_bytes"] = manifest["image_url"].map(
        lambda url: download_results[url]["file_size_bytes"]
    )

    manifest["error"] = manifest["image_url"].map(
        lambda url: download_results[url]["error"]
    )

    manifest.to_csv(MANIFEST_FILE, index=False)

    new_downloads = sum(
        result["status"] == "downloaded"
        for result in download_results.values()
    )

    already_present = sum(
        result["status"] == "already_exists"
        for result in download_results.values()
    )

    failed = sum(
        result["status"] == "failed"
        for result in download_results.values()
    )

    skipped = sum(
        result["status"] == "skipped_size_limit"
        for result in download_results.values()
    )

    print("\nIMAGE INGESTION SUMMARY")
    print("-" * 40)
    print(f"Product-image associations : {len(manifest)}")
    print(f"Unique image URLs          : {len(unique_urls)}")
    print(f"New images downloaded      : {new_downloads}")
    print(f"Already present            : {already_present}")
    print(f"Failed downloads           : {failed}")
    print(f"Skipped by size limit      : {skipped}")
    print(f"New download size          : {downloaded_bytes / (1024 * 1024):.2f} MB")
    print(f"Manifest                   : {MANIFEST_FILE}")

    if failed or skipped:
        print("\nImage ingestion completed with some unavailable images.")
        print("Check the manifest's error column for details.")
    else:
        print("\nAll unique image URLs are available locally.")

    return failed == 0 and skipped == 0


if __name__ == "__main__":
    try:
        success = download_product_images()
    except Exception as error:
        print(f"\nIMAGE INGESTION ERROR: {error}")
        success = False

    if not success:
        sys.exit(1)
