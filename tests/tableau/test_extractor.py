from metaphor.tableau.extractor import TableauExtractor


def test_view_url():
    workbook_url = "https://10ax.online.tableau.com/#/site/abc/workbooks/123/views"
    workbook_name = "Regional"
    view_name = "Obesity"

    extractor = TableauExtractor()

    extractor._get_base_url(workbook_url)
    view_url = extractor._build_view_url(workbook_name, view_name)

    assert (
        view_url == "https://10ax.online.tableau.com/#/site/abc/views/Regional/Obesity"
    )
