from config.settings import Config
from pathlib import Path
import click
from prefect.settings import get_current_settings

@click.command('init_block')
@click.option('--block_name',prompt='Ingresa el nombre del bloque ')
@click.option('--env_path',prompt='Ingresa la ruta del archivo .env ',type=click.Path(exists=True))
def main(block_name : str, env_path : Path):
    
    api_url = get_current_settings().PREFECT_API_URL

    click.echo(f'Está creando un bloque de configuración para el proyecto en el servidor : {api_url}')

    if not click.confirm("¿Desea continuar con la creación del bloque en esta dirección?"):
        click.echo("Operación cancelada por el usuario.")
        return
    
    Config.create_block_from_env(block_name,env_path,overwrite_block=True,override_env_vars=True)


    click.echo(f"Bloque '{block_name}' creado con la configuración del archivo '{env_path}' en {api_url} :)")


if __name__ == '__main__':
    main()