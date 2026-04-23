from sqlalchemy import text
from sqlalchemy.orm import Session


def upsert_dim_empresa(
    db: Session,
    company_id: int,
    company_name: str | None,
    group_company_id: int | None = None,
    group_company_name: str | None = None,
    holding_id: int | None = None,
    holding_name: str | None = None,
    subsidiary_id: int | None = None,
    subsidiary_name: str | None = None,
) -> None:
    db.execute(text("""
        insert into stg.dim_empresa (
            company_id,
            company_name,
            group_company_id,
            group_company_name,
            holding_id,
            holding_name,
            subsidiary_id,
            subsidiary_name,
            updated_at
        )
        values (
            :company_id,
            :company_name,
            :group_company_id,
            :group_company_name,
            :holding_id,
            :holding_name,
            :subsidiary_id,
            :subsidiary_name,
            now()
        )
        on conflict (company_id) do update set
            company_name = excluded.company_name,
            group_company_id = excluded.group_company_id,
            group_company_name = excluded.group_company_name,
            holding_id = excluded.holding_id,
            holding_name = excluded.holding_name,
            subsidiary_id = excluded.subsidiary_id,
            subsidiary_name = excluded.subsidiary_name,
            updated_at = now()
    """), {
        "company_id": company_id,
        "company_name": company_name,
        "group_company_id": group_company_id,
        "group_company_name": group_company_name,
        "holding_id": holding_id,
        "holding_name": holding_name,
        "subsidiary_id": subsidiary_id,
        "subsidiary_name": subsidiary_name,
    })


def upsert_dim_credor(
    db: Session,
    creditor_id: int,
    creditor_name: str | None,
    creditor_document: str | None = None,
    trade_name: str | None = None,
    cpf: str | None = None,
    cnpj: str | None = None,
    supplier: str | None = None,
    broker: str | None = None,
    employee: str | None = None,
    active: bool | None = None,
    state_registration_number: str | None = None,
    state_registration_type: str | None = None,
    tax_classification_id: int | None = None,
    tax_classification: str | None = None,
    payment_type_id: int | None = None,
    creation_date_source: str | None = None,
    last_modification_date_source: str | None = None,
    city_id: int | None = None,
    city_name: str | None = None,
    street_name: str | None = None,
    address_number: str | None = None,
    address_complement: str | None = None,
    neighborhood: str | None = None,
    state: str | None = None,
    zip_code: str | None = None
) -> None:
    db.execute(text("""
        insert into stg.dim_credor (
            creditor_id,
            creditor_name,
            creditor_document,
            trade_name,
            cpf,
            cnpj,
            supplier,
            broker,
            employee,
            active,
            state_registration_number,
            state_registration_type,
            tax_classification_id,
            tax_classification,
            payment_type_id,
            creation_date_source,
            last_modification_date_source,
            city_id,
            city_name,
            street_name,
            address_number,
            address_complement,
            neighborhood,
            state,
            zip_code,
            updated_at
        )
        values (
            :creditor_id,
            :creditor_name,
            :creditor_document,
            :trade_name,
            :cpf,
            :cnpj,
            :supplier,
            :broker,
            :employee,
            :active,
            :state_registration_number,
            :state_registration_type,
            :tax_classification_id,
            :tax_classification,
            :payment_type_id,
            :creation_date_source,
            :last_modification_date_source,
            :city_id,
            :city_name,
            :street_name,
            :address_number,
            :address_complement,
            :neighborhood,
            :state,
            :zip_code,
            now()
        )
        on conflict (creditor_id) do update set
            creditor_name = excluded.creditor_name,
            creditor_document = excluded.creditor_document,
            trade_name = excluded.trade_name,
            cpf = excluded.cpf,
            cnpj = excluded.cnpj,
            supplier = excluded.supplier,
            broker = excluded.broker,
            employee = excluded.employee,
            active = excluded.active,
            state_registration_number = excluded.state_registration_number,
            state_registration_type = excluded.state_registration_type,
            tax_classification_id = excluded.tax_classification_id,
            tax_classification = excluded.tax_classification,
            payment_type_id = excluded.payment_type_id,
            creation_date_source = excluded.creation_date_source,
            last_modification_date_source = excluded.last_modification_date_source,
            city_id = excluded.city_id,
            city_name = excluded.city_name,
            street_name = excluded.street_name,
            address_number = excluded.address_number,
            address_complement = excluded.address_complement,
            neighborhood = excluded.neighborhood,
            state = excluded.state,
            zip_code = excluded.zip_code,
            updated_at = now()
    """), {
        "creditor_id": creditor_id,
        "creditor_name": creditor_name,
        "creditor_document": creditor_document,
        "trade_name": trade_name,
        "cpf": cpf,
        "cnpj": cnpj,
        "supplier": supplier,
        "broker": broker,
        "employee": employee,
        "active": active,
        "state_registration_number": state_registration_number,
        "state_registration_type": state_registration_type,
        "tax_classification_id": tax_classification_id,
        "tax_classification": tax_classification,
        "payment_type_id": payment_type_id,
        "creation_date_source": creation_date_source,
        "last_modification_date_source": last_modification_date_source,
        "city_id": city_id,
        "city_name": city_name,
        "street_name": street_name,
        "address_number": address_number,
        "address_complement": address_complement,
        "neighborhood": neighborhood,
        "state": state,
        "zip_code": zip_code,
    })


def refresh_credor_children(
    db: Session,
    creditor_id: int,
    phones: list[dict] | None,
    contacts: list[dict] | None
) -> None:
    db.execute(text("delete from stg.credor_telefone where creditor_id = :creditor_id"), {"creditor_id": creditor_id})
    db.execute(text("delete from stg.credor_contato where creditor_id = :creditor_id"), {"creditor_id": creditor_id})

    for phone in (phones or []):
        db.execute(text("""
            insert into stg.credor_telefone (
                creditor_id, phone_type, phone_number, main, note, idd, ddd
            )
            values (
                :creditor_id, :phone_type, :phone_number, :main, :note, :idd, :ddd
            )
            on conflict do nothing
        """), {
            "creditor_id": creditor_id,
            "phone_type": phone.get("type"),
            "phone_number": phone.get("number"),
            "main": phone.get("main"),
            "note": phone.get("note"),
            "idd": phone.get("idd"),
            "ddd": phone.get("ddd"),
        })

    for contact in (contacts or []):
        db.execute(text("""
            insert into stg.credor_contato (
                creditor_id, contact_name, email, phone, note
            )
            values (
                :creditor_id, :contact_name, :email, :phone, :note
            )
            on conflict do nothing
        """), {
            "creditor_id": creditor_id,
            "contact_name": contact.get("name"),
            "email": contact.get("email"),
            "phone": contact.get("phone"),
            "note": contact.get("note"),
        })

def upsert_dim_categoria_financeira(
    db: Session,
    financial_category_id: str,
    financial_category_name: str | None,
    tp_conta: str | None = None,
    fl_redutora: str | None = None,
    fl_ativa: str | None = None,
    fl_adiantamento: str | None = None,
    fl_imposto: str | None = None
) -> None:
    db.execute(text("""
        insert into stg.dim_categoria_financeira (
            financial_category_id,
            financial_category_name,
            tp_conta,
            fl_redutora,
            fl_ativa,
            fl_adiantamento,
            fl_imposto,
            updated_at
        )
        values (
            :financial_category_id,
            :financial_category_name,
            :tp_conta,
            :fl_redutora,
            :fl_ativa,
            :fl_adiantamento,
            :fl_imposto,
            now()
        )
        on conflict (financial_category_id) do update set
            financial_category_name = excluded.financial_category_name,
            tp_conta = excluded.tp_conta,
            fl_redutora = excluded.fl_redutora,
            fl_ativa = excluded.fl_ativa,
            fl_adiantamento = excluded.fl_adiantamento,
            fl_imposto = excluded.fl_imposto,
            updated_at = now()
    """), {
        "financial_category_id": financial_category_id,
        "financial_category_name": financial_category_name,
        "tp_conta": tp_conta,
        "fl_redutora": fl_redutora,
        "fl_ativa": fl_ativa,
        "fl_adiantamento": fl_adiantamento,
        "fl_imposto": fl_imposto,
    })
    
def upsert_dim_centro_custo(
    db: Session,
    cost_center_id: int,
    cost_center_name: str | None,
    cnpj: str | None = None,
    id_company: int | None = None,
    company_id: int | None = None,
    company_name: str | None = None,
    group_company_id: int | None = None,
    group_company_name: str | None = None,
    holding_id: int | None = None,
    holding_name: str | None = None,
    subsidiary_id: int | None = None,
    subsidiary_name: str | None = None,
    parent_cost_center_id: int | None = None,
    reduced_code: str | None = None,
    classification: str | None = None,
    active: bool | None = None,
    blocked: bool | None = None,
    created_at_source: str | None = None,
    modified_at_source: str | None = None,
    observation: str | None = None
) -> None:
    db.execute(text("""
        insert into stg.dim_centro_custo (
            cost_center_id,
            cost_center_name,
            cnpj,
            id_company,
            company_id,
            company_name,
            group_company_id,
            group_company_name,
            holding_id,
            holding_name,
            subsidiary_id,
            subsidiary_name,
            parent_cost_center_id,
            reduced_code,
            classification,
            active,
            blocked,
            created_at_source,
            modified_at_source,
            observation,
            updated_at
        )
        values (
            :cost_center_id,
            :cost_center_name,
            :cnpj,
            :id_company,
            :company_id,
            :company_name,
            :group_company_id,
            :group_company_name,
            :holding_id,
            :holding_name,
            :subsidiary_id,
            :subsidiary_name,
            :parent_cost_center_id,
            :reduced_code,
            :classification,
            :active,
            :blocked,
            :created_at_source,
            :modified_at_source,
            :observation,
            now()
        )
        on conflict (cost_center_id) do update set
            cost_center_name = excluded.cost_center_name,
            cnpj = excluded.cnpj,
            id_company = excluded.id_company,
            company_id = excluded.company_id,
            company_name = excluded.company_name,
            group_company_id = excluded.group_company_id,
            group_company_name = excluded.group_company_name,
            holding_id = excluded.holding_id,
            holding_name = excluded.holding_name,
            subsidiary_id = excluded.subsidiary_id,
            subsidiary_name = excluded.subsidiary_name,
            parent_cost_center_id = excluded.parent_cost_center_id,
            reduced_code = excluded.reduced_code,
            classification = excluded.classification,
            active = excluded.active,
            blocked = excluded.blocked,
            created_at_source = excluded.created_at_source,
            modified_at_source = excluded.modified_at_source,
            observation = excluded.observation,
            updated_at = now()
    """), {
        "cost_center_id": cost_center_id,
        "cost_center_name": cost_center_name,
        "cnpj": cnpj,
        "id_company": id_company,
        "company_id": company_id,
        "company_name": company_name,
        "group_company_id": group_company_id,
        "group_company_name": group_company_name,
        "holding_id": holding_id,
        "holding_name": holding_name,
        "subsidiary_id": subsidiary_id,
        "subsidiary_name": subsidiary_name,
        "parent_cost_center_id": parent_cost_center_id,
        "reduced_code": reduced_code,
        "classification": classification,
        "active": active,
        "blocked": blocked,
        "created_at_source": created_at_source,
        "modified_at_source": modified_at_source,
        "observation": observation,
    })


def refresh_centro_custo_children(
    db: Session,
    cost_center_id: int,
    building_sectors: list[dict] | None
) -> None:
    db.execute(
        text("delete from stg.centro_custo_setor where cost_center_id = :cost_center_id"),
        {"cost_center_id": cost_center_id}
    )

    for sector in (building_sectors or []):
        db.execute(text("""
            insert into stg.centro_custo_setor (
                cost_center_id,
                building_sector_id,
                building_sector_name,
                accountable_id,
                accountable_name
            )
            values (
                :cost_center_id,
                :building_sector_id,
                :building_sector_name,
                :accountable_id,
                :accountable_name
            )
            on conflict do nothing
        """), {
            "cost_center_id": cost_center_id,
            "building_sector_id": sector.get("id"),
            "building_sector_name": sector.get("name"),
            "accountable_id": sector.get("accountableId"),
            "accountable_name": sector.get("accountableName"),
        })

def upsert_dim_cliente(
    db: Session,
    client_id: int,
    client_name: str | None,
    client_document: str | None = None,
    person_type: str | None = None,
    client_type: str | None = None,
    foreigner: str | None = None,
    international_id: str | None = None,
    cnpj: str | None = None,
    cpf: str | None = None,
    email: str | None = None,
    fantasy_name: str | None = None,
    state_registration_number: str | None = None,
    city_registration_number: str | None = None,
    contact_name: str | None = None,
    site: str | None = None,
    note: str | None = None,
    created_at_source: str | None = None,
    modified_at_source: str | None = None,
    main_phone: str | None = None,
    main_address_type: str | None = None,
    street_name: str | None = None,
    address_number: str | None = None,
    address_complement: str | None = None,
    neighborhood: str | None = None,
    city_id: int | None = None,
    city_name: str | None = None,
    state: str | None = None,
    zip_code: str | None = None
) -> None:
    db.execute(text("""
        insert into stg.dim_cliente (
            client_id,
            client_name,
            client_document,
            person_type,
            client_type,
            foreigner,
            international_id,
            cnpj,
            cpf,
            email,
            fantasy_name,
            state_registration_number,
            city_registration_number,
            contact_name,
            site,
            note,
            created_at_source,
            modified_at_source,
            main_phone,
            main_address_type,
            street_name,
            address_number,
            address_complement,
            neighborhood,
            city_id,
            city_name,
            state,
            zip_code,
            updated_at
        )
        values (
            :client_id,
            :client_name,
            :client_document,
            :person_type,
            :client_type,
            :foreigner,
            :international_id,
            :cnpj,
            :cpf,
            :email,
            :fantasy_name,
            :state_registration_number,
            :city_registration_number,
            :contact_name,
            :site,
            :note,
            :created_at_source,
            :modified_at_source,
            :main_phone,
            :main_address_type,
            :street_name,
            :address_number,
            :address_complement,
            :neighborhood,
            :city_id,
            :city_name,
            :state,
            :zip_code,
            now()
        )
        on conflict (client_id) do update set
            client_name = excluded.client_name,
            client_document = excluded.client_document,
            person_type = excluded.person_type,
            client_type = excluded.client_type,
            foreigner = excluded.foreigner,
            international_id = excluded.international_id,
            cnpj = excluded.cnpj,
            cpf = excluded.cpf,
            email = excluded.email,
            fantasy_name = excluded.fantasy_name,
            state_registration_number = excluded.state_registration_number,
            city_registration_number = excluded.city_registration_number,
            contact_name = excluded.contact_name,
            site = excluded.site,
            note = excluded.note,
            created_at_source = excluded.created_at_source,
            modified_at_source = excluded.modified_at_source,
            main_phone = excluded.main_phone,
            main_address_type = excluded.main_address_type,
            street_name = excluded.street_name,
            address_number = excluded.address_number,
            address_complement = excluded.address_complement,
            neighborhood = excluded.neighborhood,
            city_id = excluded.city_id,
            city_name = excluded.city_name,
            state = excluded.state,
            zip_code = excluded.zip_code,
            updated_at = now()
    """), {
        "client_id": client_id,
        "client_name": client_name,
        "client_document": client_document,
        "person_type": person_type,
        "client_type": client_type,
        "foreigner": foreigner,
        "international_id": international_id,
        "cnpj": cnpj,
        "cpf": cpf,
        "email": email,
        "fantasy_name": fantasy_name,
        "state_registration_number": state_registration_number,
        "city_registration_number": city_registration_number,
        "contact_name": contact_name,
        "site": site,
        "note": note,
        "created_at_source": created_at_source,
        "modified_at_source": modified_at_source,
        "main_phone": main_phone,
        "main_address_type": main_address_type,
        "street_name": street_name,
        "address_number": address_number,
        "address_complement": address_complement,
        "neighborhood": neighborhood,
        "city_id": city_id,
        "city_name": city_name,
        "state": state,
        "zip_code": zip_code,
    })

def refresh_cliente_children(
    db: Session,
    client_id: int,
    phones: list[dict] | None,
    addresses: list[dict] | None
) -> None:
    db.execute(text("delete from stg.cliente_telefone where client_id = :client_id"), {"client_id": client_id})
    db.execute(text("delete from stg.cliente_endereco where client_id = :client_id"), {"client_id": client_id})

    for phone in (phones or []):
        db.execute(text("""
            insert into stg.cliente_telefone (
                client_id, phone_type, phone_number, main, note, idd, ddd
            )
            values (
                :client_id, :phone_type, :phone_number, :main, :note, :idd, :ddd
            )
            on conflict do nothing
        """), {
            "client_id": client_id,
            "phone_type": phone.get("type"),
            "phone_number": phone.get("number"),
            "main": phone.get("main"),
            "note": phone.get("note"),
            "idd": phone.get("idd"),
            "ddd": phone.get("ddd"),
        })

    for address in (addresses or []):
        db.execute(text("""
            insert into stg.cliente_endereco (
                client_id, address_type, street_name, address_number, complement,
                neighborhood, city_id, city_name, state, zip_code, mail
            )
            values (
                :client_id, :address_type, :street_name, :address_number, :complement,
                :neighborhood, :city_id, :city_name, :state, :zip_code, :mail
            )
            on conflict do nothing
        """), {
            "client_id": client_id,
            "address_type": address.get("type"),
            "street_name": address.get("streetName"),
            "address_number": address.get("number"),
            "complement": address.get("complement"),
            "neighborhood": address.get("neighborhood"),
            "city_id": address.get("cityId"),
            "city_name": address.get("city"),
            "state": address.get("state"),
            "zip_code": address.get("zipCode"),
            "mail": address.get("mail"),
        })