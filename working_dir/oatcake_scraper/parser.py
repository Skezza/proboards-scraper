import logging
import re
from dataclasses import dataclass, asdict
from typing import Iterable, List, Optional
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BOARD_ID_PATTERN = re.compile(r"nav-tree-board-(\d+)")
THREAD_ID_PATTERN = re.compile(r"thread-(\d+)")
POST_ID_PATTERN = re.compile(r"post-(\d+)")


@dataclass
class BoardInfo:
    board_id: str
    name: str
    url: str
    category: Optional[str] = None
    description: Optional[str] = None


@dataclass
class ThreadSummary:
    thread_id: str
    title: str
    url: str
    replies: Optional[int]
    views: Optional[int]
    last_post_time: Optional[str]


@dataclass
class PostData:
    post_id: str
    author: Optional[str]
    timestamp: Optional[str]
    timestamp_ms: Optional[int]
    content: Optional[str]
    raw_html: str


@dataclass
class BoardPageData:
    board: BoardInfo
    threads: List[ThreadSummary]
    current_page: int
    last_page: int


@dataclass
class ThreadPageData:
    posts: List[PostData]
    current_page: int
    last_page: int


def _extract_page_number(href: str) -> Optional[int]:
    parsed = urlparse(href)
    params = parse_qs(parsed.query)
    page_values = params.get("page")
    if page_values:
        try:
            return int(page_values[0])
        except ValueError:
            return None
    return None


def _pagination_bounds(soup: BeautifulSoup) -> (int, int):
    pagination = soup.select_one("ul.ui-pagination")
    if not pagination:
        return 1, 1
    pages = {}
    current_page = 1
    for li in pagination.find_all("li"):
        a = li.find("a")
        if a and a.text.strip().isdigit():
            page_no = int(a.text.strip())
            pages[page_no] = page_no
        elif "state-selected" in li.get("class", []):
            text = a.text.strip() if a else li.get_text(strip=True)
            if text.isdigit():
                current_page = int(text)
    last_page = max(pages.keys()) if pages else current_page
    return current_page, last_page


def discover_boards(html: str, base_url: str) -> List[BoardInfo]:
    soup = BeautifulSoup(html, "html.parser")
    catalog = {}
    for cat in soup.select("li[class*='nav-tree-cat']"):  # categories contain boards
        category_name = (
            cat.select_one("span.item-text").get_text(strip=True)
            if cat.select_one("span.item-text")
            else None
        )
        for board_li in cat.select("li[class*='nav-tree-board']"):  # find nested board entries
            class_list = board_li.get("class", [])
            board_id = None
            for cls in class_list:
                match = BOARD_ID_PATTERN.search(cls)
                if match:
                    board_id = match.group(1)
                    break
            if not board_id:
                continue
            anchor = board_li.find("a")
            if not anchor:
                continue
            url = urljoin(base_url, anchor.get("href", ""))
            name = anchor.get_text(strip=True)
            if not name:
                continue
            if board_id in catalog:
                if catalog[board_id].category is None and category_name:
                    catalog[board_id].category = category_name
                continue
            catalog[board_id] = BoardInfo(
                board_id=board_id,
                name=name,
                url=url,
                category=category_name,
            )
    return list(catalog.values())


def parse_board_page(html: str, board: BoardInfo, base_url: str) -> BoardPageData:
    soup = BeautifulSoup(html, "html.parser")
    heading = soup.select_one("div.container.threads h1")
    if heading:
        board.name = heading.get_text(strip=True)
    meta_desc = soup.find("meta", property="og:description")
    if meta_desc and meta_desc.get("content"):
        board.description = meta_desc["content"].strip()
    current_page, last_page = _pagination_bounds(soup)
    threads = []
    for row in soup.find_all("tr", id=THREAD_ID_PATTERN):
        thread_id_attr = row.get("id", "")
        match = THREAD_ID_PATTERN.match(thread_id_attr)
        if not match:
            continue
        thread_id = match.group(1)
        anchor = row.select_one("a.thread-link")
        if not anchor:
            continue
        title = anchor.get_text(strip=True)
        href = anchor.get("href", "")
        url = urljoin(base_url, href)
        replies = _parse_int(row.select_one("td.replies"))
        views = _parse_int(row.select_one("td.views"))
        last_post_time = (
            row.select_one("td.latest a abbr").get("title")
            if row.select_one("td.latest a abbr")
            else None
        )
        threads.append(
            ThreadSummary(
                thread_id=thread_id,
                title=title,
                url=url,
                replies=replies,
                views=views,
                last_post_time=last_post_time,
            )
        )
    return BoardPageData(
        board=board,
        threads=threads,
        current_page=current_page,
        last_page=last_page,
    )


def _parse_int(element) -> Optional[int]:
    if element is None:
        return None
    text = element.get_text(strip=True).replace(",", "")
    try:
        return int(text)
    except ValueError:
        return None


def parse_thread_page(html: str) -> ThreadPageData:
    soup = BeautifulSoup(html, "html.parser")
    current_page, last_page = _pagination_bounds(soup)
    posts = []
    for row in soup.select("tr[id^=post-]"):
        post_id_attr = row.get("id", "")
        match = POST_ID_PATTERN.match(post_id_attr)
        if not match:
            continue
        post_id = match.group(1)
        author_elem = row.select_one("a.o-user-link")
        author = author_elem.get_text(strip=True) if author_elem else None
        timestamp_elem = row.select_one("abbr.o-timestamp")
        timestamp = timestamp_elem.get("title") if timestamp_elem else None
        timestamp_ms = (
            int(timestamp_elem.get("data-timestamp")) if timestamp_elem and timestamp_elem.get("data-timestamp") else None
        )
        message_elem = row.select_one("div.message")
        content = message_elem.get_text("\n", strip=True) if message_elem else None
        raw_html = str(message_elem) if message_elem else ""
        posts.append(
            PostData(
                post_id=post_id,
                author=author,
                timestamp=timestamp,
                timestamp_ms=timestamp_ms,
                content=content,
                raw_html=raw_html,
            )
        )
    return ThreadPageData(posts=posts, current_page=current_page, last_page=last_page)


def to_dict(obj):
    if hasattr(obj, "__dataclass_fields__"):
        return asdict(obj)
    if isinstance(obj, list):
        return [to_dict(item) for item in obj]
    return obj
