
from lxml import etree
from lxml.etree import ParserError


def parse_qa_feedback(feedback_cnt):

    qa_outcomes = []
    #print(feedback_cnt)
    
    try:
        parser = etree.HTMLParser()
        tree = etree.parse(feedback_cnt, parser)
    except ParserError as e:
        print(e)

    print(tree)
    rows = tree.xpath("//td[@class='bullet']/ancestor::"
                      "*[position()=1]")

    #rows = tree.xpath("//td[@class='bullet']")
    print(f'Found {len(rows)} rows')

    for r in rows:
        err_levl = r.xpath("./td[@class='bullet']/div/@class")
        err_code = r.xpath("./td[@class='bullet']/div/a/text()")
        err_mesg = r.xpath("./td/span[@class='largeText']/text()")
        if len(err_mesg) == 0:
            err_mesg = ['']

        if len(err_code) > 0:
            qa_outcomes.append((err_code[0], err_levl[0], err_mesg[0]))
    return qa_outcomes


if __name__ == '__main__':


    res = parse_qa_feedback('/home/luca/Downloads/view.html')
    print(res)