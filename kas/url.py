from dataclasses import dataclass
from typing import ClassVar
from urllib.parse import urlencode, urlunsplit


@dataclass
class KAS_URL:
    _BASE_URL: ClassVar[str] = "kas.gov.pl"
    _QUERY: ClassVar[dict[str, str]] = {
        "p_p_id": "101_INSTANCE_Uvm3",
        "p_p_lifecycle": "0",
        "p_p_state": "normal",
        "p_p_mode": "view",
        "p_p_col_id": "column-2",
        "p_p_col_count": "1",
        "_101_INSTANCE_Uvm3_delta": "20",
        "_101_INSTANCE_Uvm3_advancedSearch": "false",
        "_101_INSTANCE_Uvm3_andOperator": "true",
    }

    voivodship: str
    city_loc: str

    @property
    def path(self) -> str:
        return f"/izba-administracji-skarbowej-w-{self.city_loc}/ogloszenia/obwieszczenia-o-licytacjach"

    @property
    def netloc(self) -> str:
        return f"www.{self.voivodship}.{self._BASE_URL}"

    def get_listing_url(self, page: int) -> str:
        return urlunsplit(
            (
                "https",
                self.netloc,
                self.path,
                urlencode(
                    {
                        **self._QUERY,
                        "cur": page,
                    }
                ),
                "",
            )
        )

MAZ_URL = KAS_URL(
    voivodship="mazowieckie",
    city_loc="warszawie",
)
KUJ_POM_URL = KAS_URL(
    voivodship="kujawsko-pomorskie",
    city_loc="bydgoszczy",
)