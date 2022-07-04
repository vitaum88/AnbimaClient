import click
import logging
import pandas as pd
from anbima import AnbimaClient
from datetime import date as dt
from datetime import timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
KEYS = [
    "codigo_ativo",
    "data_vencimento",
    "percentual_taxa",
    "data_referencia",
    "taxa_compra",
    "taxa_venda",
    "taxa_indicativa",
    "duration",
    "emissor",
]


@click.command()
@click.argument("client_id")
@click.argument("client_secret")
@click.option("-r", "--recursive", is_flag=True, default=False)
@click.option("-d", "--date", type=click.DateTime(formats=["%Y-%m-%d"]))
def main(client_id, client_secret, recursive, date=None):
    logger.info(
        f"Starting fetch for {client_id=} "
        f"with the following params: \n"
        f"date={date.strftime('%Y-%m-%d')}, {recursive=}"
    )
    client = AnbimaClient(
        client_id=client_id, client_secret=client_secret
    )
    client.connect()
    filter_deb = []
    if date:
        delta = dt.today() - date.date()
        for single_date in (date + timedelta(n) for n in range(delta.days)):
            deb = client.debentures.secondary(date=single_date.strftime('%Y-%m-%d'))
            filter_deb += list(map(lambda x: {k: x.get(k, None) for k in KEYS}, deb))

            if not recursive:
                break
        logger.info("Finished fetching data. Generating output file.")
        #pd.DataFrame(filter_deb).to_excel(f"output.xlsx", index=False)
        pd.DataFrame(filter_deb).to_csv(f"output.csv", encoding="utf-8", index=False)


if __name__=="__main__":
    main()
