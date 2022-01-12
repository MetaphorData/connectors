from metaphor.tableau.extractor import TableauExtractor


def test_view_url():
    workbook_url = "https://10ax.online.tableau.com/#/site/abc/workbooks/123/views"
    view_name = "Regional/sheets/Obesity"

    extractor = TableauExtractor()

    extractor._get_base_url(workbook_url)
    view_url = extractor._build_view_url(view_name)

    assert (
        view_url == "https://10ax.online.tableau.com/#/site/abc/views/Regional/Obesity"
    )
