import requests
import time
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Base URL of your ProBoards forum. Update this for your specific forum.
BASE_URL = "https://exampleforum.proboards.com"

# Initialize a requests session with a polite User-Agent.
session = requests.Session()
session.headers.update({
    "User-Agent": "ForumArchiveBot/1.0 (approved archive)"
})

def get_soup(url: str) -> BeautifulSoup:
    """
    Fetch a page and return a parsed BeautifulSoup object.
    Includes a delay between requests to respect server load.
    """
    response = session.get(url, timeout=20)
    response.raise_for_status()
    # Polite delay between requests
    time.sleep(1)
    return BeautifulSoup(response.text, "html.parser")

def get_thread_links(board_url: str) -> list[str]:
    """
    Collect thread URLs from a board page.
    This uses a CSS selector for thread links. Adjust the selector
    as necessary based on your forum's theme.
    """
    soup = get_soup(board_url)
    threads = []
    for a in soup.select("a.thread-link"):
        href = a.get("href")
        if href:
            threads.append(urljoin(BASE_URL, href))
    return list(set(threads))

def parse_thread(thread_url: str) -> list[dict]:
    """
    Iterate through pages of a thread and extract post data.
    Returns a list of dictionaries with author, date, and content.
    """
    posts = []
    page = 1
    while True:
        url = f"{thread_url}/page/{page}"
        soup = get_soup(url)
        post_blocks = soup.select(".post")
        if not post_blocks:
            break
        for post in post_blocks:
            author_elem = post.select_one(".user-link")
            date_elem = post.select_one(".date")
            content_elem = post.select_one(".message")
            posts.append({
                "author": author_elem.text.strip() if author_elem else None,
                "date": date_elem.text.strip() if date_elem else None,
                "content": content_elem.get_text("\n", strip=True) if content_elem else None
            })
        page += 1
    return posts

def archive_board(board_url: str, output_path: str = "archive.json") -> None:
    """
    Archive all threads from a board and save them to a JSON file.
    The output file contains a list of thread data structures with their posts.
    """
    threads = get_thread_links(board_url)
    archive = []
    for thread in threads:
        print(f"Archiving: {thread}")
        posts = parse_thread(thread)
        archive.append({
            "thread_url": thread,
            "posts": posts
        })
    with open(output_path, "w", encoding="utf8") as f:
        json.dump(archive, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    # Example usage: update `board_url` to the board you want to archive.
    board_url = BASE_URL + "/board/1/general-discussion"
    archive_board(board_url)
