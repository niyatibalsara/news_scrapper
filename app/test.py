import requests
url = 'https://www.thehindu.com/news/national/karnataka/display-of-lpg-cng-prices-and-stock-made-mandatoryin-dharwad-district-of-karnataka/article70854113.ece'
r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)', 'Referer': 'https://www.google.com/'})
print('Status:', r.status_code)
print('Length:', len(r.text))
# Count paragraphs
from bs4 import BeautifulSoup
soup = BeautifulSoup(r.text, 'lxml')
paras = soup.find_all('p')
print('Paragraphs found:', len(paras))
for i, p in enumerate(paras[:10]):
    print(f'  P{i}:', p.get_text(strip=True)[:100])
"