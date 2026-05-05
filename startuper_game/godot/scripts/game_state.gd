extends RefCounted
class_name GameState

const TOTAL_WEEKS := 18
const TASKS_PER_WEEK := 3

## Человекочитаемые подсказки для обучающего режима (вертикальный срез).
const EFFECT_LABELS: Dictionary = {
	"cash": "Деньги",
	"revenue": "MRR",
	"burn_rate": "Burn (ежемес.)",
	"activation": "Активация",
	"retention": "Удержание",
	"morale": "Мораль",
	"tech_debt": "Техдолг",
	"reputation": "Репутация",
	"hire_marketer": "Найм CMO (маркетинг и рост)",
	"hire_cto": "Найм CTO (продукт и разработка)",
	"hire_cfo": "Найм CFO (финансы и burn)",
	"hire_hr": "Найм HR (найм и культура)",
	"hire_coo": "Найм COO (операции и процессы)",
	"hire_cco": "Найм CCO (продажи и выручка)",
	"hire_cpo": "Найм CPO (продукт и метрики)",
	"hire_friend": "Найм Друга",
	"startup_fellows_onboard": "Запуск Startup Fellows",
	"rent_office_b": "Аренда офиса B",
	"rent_office_a": "Аренда офиса A",
	"grant_apply": "Заявка на грант",
	"grant_report_submit": "Сдать отчётность по гранту",
	"accelerator_apply": "Заявка в акселератор",
	"smoothie_bar": "Смузи-бар в офисе",
	"coffee_lady": "Кофе-леди в офисе",
	"office_masseur": "Массажист в офисе",
	"advisor_pipeline": "Линия эдвайзеров (эксперты в нежданчиках)",
	"growth_agency_contract": "Контракт с агентством",
	"growth_agency_churn": "Расторжение с агентством",
	"influencer_channel": "Канал инфлюенсеров",
	"startup_telegram_channel": "Telegram о стартапе (ведёте канал)",
	"startup_telegram_channel_close": "Закрыть Telegram о стартапе",
	"fire_marketer": "Уволить CMO (маркетинг и рост)",
	"fire_cto": "Уволить CTO (продукт и разработка)",
	"fire_cfo": "Уволить CFO (финансы и burn)",
	"fire_hr": "Уволить HR (найм и культура)",
	"fire_coo": "Уволить COO (операции и процессы)",
	"fire_cco": "Уволить CCO (продажи и выручка)",
	"fire_cpo": "Уволить CPO (продукт и метрики)",
	"fire_friend": "Уволить Друга",
	"custdev_points": "CustDev",
	"problem_clarity": "Проблема",
	"mvp_progress": "MVP",
	"launch_market": "Вывод на рынок",
	"founder_share": "Доля Мамкина",
}

const EFFECT_ORDER: Array[String] = [
	"cash", "revenue", "burn_rate", "activation", "retention",
	"morale", "tech_debt", "reputation", "founder_share",
	"grant_apply", "accelerator_apply", "advisor_pipeline",
	"autoplay_vacation", "autoplay_vacation_friend", "autoplay_friend_pilot", "friend_resentment",
]

static func describe_effects(effects: Dictionary) -> String:
	var parts: Array[String] = []
	var seen: Dictionary = {}
	for stat: String in EFFECT_ORDER:
		if effects.has(stat):
			parts.append(_describe_effect_line(stat, float(effects[stat])))
			seen[stat] = true
	for stat: String in effects:
		if not seen.has(stat) and stat != "playable_prototype" and stat != "technical_mvp" and stat != "accelerator_accept_program":
			parts.append(_describe_effect_line(stat, float(effects[stat])))
	var out := ""
	for i in parts.size():
		if i > 0:
			out += " · "
		out += parts[i]
	return out

## Пороги «цепочки продукта» (ачивки в UI): сегменты → инсайты → прототип → полевые тесты.
## Шаг «прототип» привязан к флагу has_playable_prototype (сборка MVP-задачами), не к сырому mvp_progress.
const CHAIN_SEGMENTS_NEED_CUSTDEV := 2.0
const CHAIN_INSIGHTS_NEED_CUSTDEV := 4.0
const CHAIN_INSIGHTS_ALT_PROBLEM := 3.0
const CHAIN_FIELD_TESTS_NEED_PROBLEM := 2.0

const TASK_PRODUCT_LANDING := "Сделать посадочную страницу продукта"
const TASK_PRODUCT_LANDING_REDO := "Переделать посадочную страницу продукта"
const TASK_TECHNICAL_MVP := "Сделать технический MVP (backend + база)"
const TASK_PRODUCT_ANALYTICS := "Подключить продуктовую аналитику (3 ключевых события)"
const TASK_CLICKABLE_PROTOTYPE_ASSEMBLY := "Собрать кликабельный прототип MVP"
## После первого успешного «Собрать кликабельный прототип MVP» в спринте показывается эта формулировка.
const TASK_CLICKABLE_PROTOTYPE_REBUILD := "Пересобрать кликабельный MVP"

## Синергии спринта (этапы Build и Fit): зелёная пара / синий тройник — визуал на карточках.
const SYNERGY_PAIRS_BUILD: Array = [
	["Desk research: черновик ICP и сегменты", "Воркшоп: сформулировать и проверить 3 ключевые гипотезы"],
	["10 холодных касаний: кастдев-письма сегменту", "Провести 5 CustDev-интервью"],
	["Провести 5 CustDev-интервью", "Синтезировать инсайты CustDev и описать JTBD"],
	["Синтезировать инсайты CustDev и описать JTBD", "Прописать ценностное предложение и позиционирование"],
	["Прописать ценностное предложение и позиционирование", TASK_CLICKABLE_PROTOTYPE_ASSEMBLY],
	[TASK_CLICKABLE_PROTOTYPE_ASSEMBLY, "Проверить прототип на 5 пользователях"],
	[TASK_TECHNICAL_MVP, TASK_PRODUCT_ANALYTICS],
	[TASK_PRODUCT_LANDING, "Юридический мини-аудит политики данных и оферты"],
	["Найм сотрудников на Startup Fellows", "Нанять HR в команду (найм и культура)"],
	["Ретро спринта: приоритизация backlog (RICE)", TASK_CLICKABLE_PROTOTYPE_ASSEMBLY],
	["Ретро спринта: приоритизация backlog (RICE)", TASK_TECHNICAL_MVP],
]
const SYNERGY_TRIPLES_BUILD: Array = [
	["Провести 5 CustDev-интервью", "Синтезировать инсайты CustDev и описать JTBD", "Прописать ценностное предложение и позиционирование"],
	["Прописать ценностное предложение и позиционирование", TASK_CLICKABLE_PROTOTYPE_ASSEMBLY, "Проверить прототип на 5 пользователях"],
]

## Пилот продукта (Fit): связанные задачи — та же подсветка синергии, что на Build.
const TASK_FIT_PILOT_SELECT := "Пилот: отбор компаний и KPI внедрения"
const TASK_FIT_PILOT_SUPPORT := "Пилот: сопровождение и дашборд метрик"
const TASK_FIT_PILOT_ITERATE := "Пилот: итерация продукта по полевому фидбеку"
const TASK_GRANT_KPI_PACK := "Подготовить пакет KPI и артефактов по гранту"
const TASK_GRANT_STATUS_SYNC := "Синхронизация с грантодателем: статус-апдейт и риски"
const TASK_GRANT_FINAL_AUDIT := "Закрыть финальный аудит грантового трека"
const TASK_LEGAL_FIT := "Legal-fit: обновить оферту, договор и SLA под реальных клиентов"
const TASK_COMPLIANCE_SCALE := "Compliance для enterprise: DPA/SLA и безопасность данных"
const TASK_RICE_FIT := "Ретро PMF-спринта: пересобрать приоритеты по RICE"
const TASK_RICE_SCALE := "Операционный ритм Scale: QBR и недельный performance review"
const TASK_ADVISOR_FIT := "Эдвайзер-сессия по PMF: уточнить ICP и фокус сегмента"
const TASK_ADVISOR_SCALE := "Advisory board: стратегия масштабирования и юнит-экономика"
const TASK_PARTNER_FIT := "Пилотные партнёрства: 3 встречи по интеграциям"
const TASK_PARTNER_SCALE := "Пакет для партнёров: кейсы, прайс и co-sell playbook"
const TASK_OFFICE_FIT := "Культура команды: ритуалы и SLA коммуникаций после роста"
const TASK_OFFICE_SCALE := "Офисная система адаптации новых сотрудников"
const SYNERGY_PAIRS_FIT: Array = [
	[TASK_FIT_PILOT_SELECT, TASK_FIT_PILOT_SUPPORT],
	[TASK_FIT_PILOT_SUPPORT, TASK_FIT_PILOT_ITERATE],
	[TASK_FIT_PILOT_SELECT, TASK_FIT_PILOT_ITERATE],
]
const SYNERGY_TRIPLES_FIT: Array = [
	[TASK_FIT_PILOT_SELECT, TASK_FIT_PILOT_SUPPORT, TASK_FIT_PILOT_ITERATE],
]

## Одобренный грант: множитель к сумме из заявки (превью спринта использует тот же множитель).
const GRANT_WIN_CASH_MULTIPLIER: float = 10.0
## Нежданчики с угрозой доле / увольнению: выше вес при низкой репутации.
const TRUST_CRISIS_EVENT_WEIGHT_REP_0: float = 2.2
const TRUST_CRISIS_EVENT_WEIGHT_REP_100: float = 0.38

## Цепочка Fit: шаги к PMF и переходу на Scale (см. get_stage_info target).
const FIT_CHAIN_ACTIVATION_MILE := 30.0
const FIT_CHAIN_RETENTION_MILE := 28.0
const FIT_CHAIN_MORALE_MILE := 35.0
const FIT_CHAIN_ACTIVATION_TARGET := 40.0
const FIT_CHAIN_RETENTION_TARGET := 34.0
const FIT_CHAIN_MORALE_TARGET := 40.0

## Цепочка Scale: экономика и бренд перед финалом квартала.
const SCALE_CHAIN_REVENUE_MILE := 3500.0
const SCALE_CHAIN_REVENUE_STRONG := 5500.0
const SCALE_CHAIN_RUNWAY_MIN := 4.0
const SCALE_CHAIN_REPUTATION_MILE := 50.0
const SCALE_CHAIN_REPUTATION_TARGET := 55.0

## Набор MVP в карточках спринта скрывается, когда прогресс уже достаточен для релиза (как в launch).
const MVP_ASSEMBLY_TASK_HIDE_FROM := 7.0

## Одинаковые по смыслу задачи на следующем этапе (task_chain) сильнее на старте этапа.
const REPEAT_CHAIN_STAGE_MULT := 1.25

const CHAIN_SCALABLE_EFFECT_KEYS: Array[String] = [
	"cash", "revenue", "burn_rate", "morale", "tech_debt", "activation", "retention", "reputation",
	"custdev_points", "problem_clarity", "mvp_progress",
]
## Старый мгновенный шанс не используется: вердикт гранта — в пятничном нежданчике (см. _grant_win_probability).

const PREREQ_METRIC_NAMES: Dictionary = {
	"custdev_points": "CustDev (кто пользователи)",
	"problem_clarity": "ясность проблемы",
	"mvp_progress": "прогресс прототипа/MVP",
	"playable_prototype": "собранный прототип/MVP для показа",
	"product_landing": "посадочная страница продукта",
	"research_memory": "запас инсайтов в памяти",
	"activation": "активация",
	"retention": "удержание",
}

## Расширенные пояснения для карточек каналов (показываются при найме CMO).
const CMO_CHANNEL_NOTES: Dictionary = {
	"Нанять digital-агентство на GTM-спринт (реклама, креатив, отчётность)":
	"Канал: подрядчик на performance и креатив. На Fit важно зафиксировать гипотезы, крео и ставки; не раздувать CAC до стабильного удержания.",
	"Масштабировать performance-маркетинг":
	"Канал: paid-скейл. Докручиваем ставки, структуру кампаний и креативные связки; следим за payback и выгоранием аудиторий.",
	"Нанять growth-агентство: performance, медиапланирование и аналитика":
	"Канал: агентство с медиапланом и атрибуцией. На Scale нужны единые дашборды, частотные капы и контроль комиссий/бонусов.",
	"Открыть B2B канал продаж":
	"Канал: прямые продажи и партнёрский pipeline. Связка с CCO: квалификация лидов, цикл сделки и прогноз воронки.",
	"Запустить PR-кампанию на рынок":
	"Канал: PR и медиа. Даёт охват и доверие; измеряем всплески трафика, упоминания и влияние на входящие лиды.",
	"Сетка performance: ремаркетинг + lookalike + частотные капы":
	"Канал: ремаркетинг и lookalike в соцсетях. Ключ — сегменты, частотные капы и анти-усталость от креатива.",
	"Контекстная реклама по high-intent запросам (Search)":
	"Канал: поиск с высоким намерением. Качество семантики и минус-слов важнее сырого бюджета.",
	"Медийный охват: баннеры и натив под узнаваемость (CPM)":
	"Канал: медийка (CPM/охват). На Scale балансируем узнаваемость и стоимость касания; связка с ретаргетом.",
	"Аффилиаты и партнёры: CPA/RevShare и промо-коды":
	"Канал: партнёрки и CPA. Нужны правила атрибуции, антифрод и прозрачные выплаты.",
	"Маркетплейсы и каталоги: листинг + отзывы (G2/Capterra и аналоги)":
	"Канал: каталоги и отзывы B2B. Долгий эффект на доверие; важны кейсы, рейтинг и ответы на отзывы.",
	"Lifecycle-email: триггеры, дайджест, double opt-in и compliance":
	"Канал: email lifecycle. Триггеры, сегменты, compliance; усиливает удержание и повторные оплаты.",
	"Холодные email-цепочки (S sequences) по ICP в B2B":
	"Канал: холодный outbound email. Персонализация, объём, доставляемость и handoff в sales.",
	"Outbound в LinkedIn: Sales Nav, тёплые интро и follow-up":
	"Канал: LinkedIn outbound. Квоты, сообщения, интро; синхронизация с CCO по статусам сделок.",
	"Спонсорство подкастов и отраслевых рассылок (нишевые медиа)":
	"Канал: нишевые медиа и подкасты. Долгий бренд-эффект; отслеживаем промокоды и всплески трафика.",
	"SEO: контент-кластеры, обновления и long-tail под спрос":
	"Канал: органический поиск. Кластеры контента и обновления страниц; лаг эффекта, зато без ставок за клик.",
	"Вебинары как воронка: лиды, запись, nurture после эфира":
	"Канал: вебинары. Лиды → дозревание → демо; важна запись, follow-up и качество Q&A.",
	"Push и in-app кампании: реактивация, апсейл, сегменты":
	"Канал: push/in-app. Сегменты, лимиты частоты и персонализация, чтобы не бить по удержанию.",
	"Реферальная программа для SMB: двусторонний бонус и трекинг":
	"Канал: рефералка. Двусторонние бонусы, трекинг инвайтов и защита от накруток.",
	"UGC в коротком видео: креаторы, брифы, тест креативов":
	"Канал: UGC и короткие ролики. Брифы, тест крео и права на контент; риск выгорания креаторов.",
	"Канал интеграций: co-marketing с платформами и партнёрские вебинары":
	"Канал: партнёрский co-marketing. Совместные вебинары и интеграции в экосистеме платформ.",
}

static func _describe_effect_line(stat: String, delta: float) -> String:
	var label: String = str(EFFECT_LABELS.get(stat, stat))
	var d: int = int(round(delta))
	var sgn: String = "+" if d >= 0 else ""
	match stat:
		"cash":
			return "%s %s%d ₽ (сразу)" % [label, sgn, d]
		"revenue":
			return "%s %s%d ₽/мес к MRR" % [label, sgn, d]
		"burn_rate":
			return "%s %s%d ₽/мес к расходам" % [label, sgn, d]
		"hire_marketer", "hire_cto", "hire_cfo", "hire_hr", "hire_coo", "hire_cco", "hire_cpo":
			return "%s: роль в команде" % label
		"hire_friend":
			return "%s: +1 в команде (дорого и с долей)" % label
		"startup_fellows_onboard":
			return "Открыт контур найма команды (кроме HR)"
		"rent_office_b":
			return "Офис B: рост морали, умеренный рост burn"
		"rent_office_a":
			return "Офис A: сильный рост морали, высокий рост burn"
		"grant_apply":
			return "Грант: вердикт на пятнице (сильный продукт ↑ шанс), до %d ₽ без доли" % d
		"grant_report_submit":
			return "Сдать отчётность по гранту (%d ₽), результат — на пятничном нежданчике" % d
		"accelerator_apply":
			return "Акселератор: отбор на пятнице (слабый продукт ↑ шанс попасть); при отборе — нагрузка и эдвайзеры"
		"advisor_pipeline":
			return "Открывает нежданчики с эдвайзерами"
		"accelerator_accept_program":
			return "Старт программы акселератора (3 недели нагрузки, затем бонус к продукту)"
		"growth_agency_contract":
			return "Контракт с digital/growth-агентством (возможны нежданчики со счетами)"
		"growth_agency_churn":
			return "Контракт с агентством расторгнут"
		"influencer_channel":
			return "Активирован канал инфлюенсеров/креаторов"
		"startup_telegram_channel":
			return "Запущен публичный Telegram о стартапе (риск нежданчиков про канал)"
		"startup_telegram_channel_close":
			return "Канал удалён; задачу «начать вести…» снова можно взять в спринт"
		"fire_marketer", "fire_cto", "fire_cfo", "fire_hr", "fire_coo", "fire_cco", "fire_cpo":
			return "%s: -burn, но риск по морали" % label
		"fire_friend":
			return "%s: -burn; доля не возвращается" % label
		"launch_market":
			return "Переход Build→Fit (после тех. MVP или MVP≥7 без одного только кликабельного макета)"
		"autoplay_vacation":
			return "След. неделя без спринта: Мамкин в отпуске (хроника + такт кассы автоматически)"
		"autoplay_vacation_friend":
			return "След. неделя без спринта: отпуск, у руля друг (плохие решения автоматически)"
		"autoplay_friend_pilot":
			return "След. неделя без спринта: друг рулит стартапом (плохие решения автоматически)"
		"friend_resentment":
			return "Обида друга +%d (позже может просить долю)" % int(round(delta))
		"founder_share":
			return "%s %s%d п.п." % [label, sgn, d]
		_:
			return "%s %s%d п." % [label, sgn, d]

var week: int = 1
var cash: float = 120000.0
var revenue: float = 0.0
var burn_rate: float = 5000.0
var morale: float = 55.0
var tech_debt: float = 10.0
var activation: float = 0.0
var retention: float = 0.0
var reputation: float = 20.0
var has_marketer: bool = false
var has_cto: bool = false
var has_cfo: bool = false
var has_hr: bool = false
var has_coo: bool = false
## CCO — руководитель отдела продаж (Chief Commercial Officer). Карточка с этапа Build, перки сильнее после выхода в рынок.
var has_cco: bool = false
## CPO — продуктолог (Chief Product Officer): в пульсе — детальные продуктовые метрики.
var has_cpo: bool = false
var friend_count: int = 0
var startup_fellows_enabled: bool = false
## 0 = нет офиса, 1 = B, 2 = A.
var office_tier: int = 0
## Был ли реальный инвест-контакт (первые деньги от инвестора/бриджа).
var has_investor_contact: bool = false
## Игрок заключал контракт с digital/growth-агентством (нежданчик «счёт сверх сметы»).
var has_growth_agency_contract: bool = false
## Игрок запускал канал с инфлюенсерами/креаторами (нежданчик про пересмотр спонсорства).
var has_influencer_channel: bool = false
## Активен публичный Telegram о стартапе (задача «Начать вести…»); после закрытия канала — снова false.
var has_startup_telegram_channel: bool = false
var has_smoothie_bar: bool = false
var has_coffee_lady: bool = false
var has_office_masseur: bool = false
var has_seen_cash_below_100k: bool = false
## В спринтах уже были задачи с CustDev-контактом.
var has_custdev_activity: bool = false
## Уже делали шаги по поиску/контакту с экспертами.
var has_expert_search_activity: bool = false
## Уже проводили интервью и вели заметки.
var has_interview_activity: bool = false
## В истории был софаундер-друг с долей (конфликты гипотез возможны даже позже).
var has_friend_equity_partner: bool = false
## Счётчики продуктового трека (для дашборда ачивок).
var stat_custdev_interviews_done: int = 0
var stat_pilot_steps_done: int = 0
var stat_prototype_users_tested: int = 0
var stat_cold_outreach_count: int = 0
var stat_expert_interviews_count: int = 0
var custdev_points: float = 0.0
var problem_clarity: float = 0.0
var mvp_progress: float = 0.0
## Есть собранный кликабельный или технический MVP (или продукт уже в рынке) — можно очные тесты на пользователях.
var has_playable_prototype: bool = false
## Уже успешно выполняли задачу «Собрать кликабельный прототип MVP» — в бэклоге показываем «Пересобрать кликабельный MVP».
var completed_assemble_clickable_mvp_task_once: bool = false
## Есть бэкенд/данные (технический MVP) — аналитика даёт продуктовый сигнал; без этого только расходы на «макет».
var has_technical_mvp: bool = false
## После задачи посадочной: открывает юр. аудит; база для оффера «переделать лендинг».
var has_product_landing: bool = false
## CustDev и Problem на момент последней сдачи посадочной (рост метрик → задача переделки).
var landing_copy_cd: float = 0.0
var landing_copy_pr: float = 0.0
var launched_to_market: bool = false
var research_memory: float = 0.0
## Доля основателя (Мамкина), %. Остальное — команда, пул опционов, инвесторы (в модели одна строка).
var founder_share_percent: float = 100.0
const BASE_BURN: float = 5000.0
const CMO_BURN: float = 2200.0
const CTO_BURN: float = 3200.0
const CFO_BURN: float = 1800.0
const HR_BURN: float = 1600.0
const COO_BURN: float = 2600.0
const CCO_BURN: float = 2400.0
const CPO_BURN: float = 2300.0
const FRIEND_BURN: float = 2800.0
const OFFICE_B_BURN: float = 2500.0
const OFFICE_A_BURN: float = 7000.0

var game_over: bool = false
var game_over_reason: String = ""
## Ключ для иллюстрации и совета на экране конца игры: bankrupt | dilution | burnout | triumph | struggle | scold
var game_over_art_key: String = "burnout"
var last_team_bonus_log: String = ""

## Грант: заявка подана в спринте; вердикт — на следующей пятнице.
var pending_grant_review: bool = false
var pending_grant_amount: float = 0.0
## Сколько раз уже выдали грант (деньги по вердикту). Повторная заявка слабее без очень сильного продукта.
var grant_wins_count: int = 0
## После одобрения гранта: пока отчётность не принята, в спринтах обязателен отчётный слот.
var grant_report_required: bool = false
var grant_report_amount: float = 0.0
var pending_grant_report_review: bool = false
var pending_grant_report_amount: float = 0.0
## Кол-во повторных попыток после провалов (каждый повтор слегка повышает шанс успеха).
var grant_report_retry_count: int = 0
## Акселератор: заявка подана; отбор — на следующей пятнице.
var pending_accelerator_review: bool = false
## Недели «хвоста» программы (мораль ↓, в конце бонус к продукту).
var accelerator_program_weeks_left: int = 0
## Открывает нежданчики с типом «Адвайзер» (акселератор / митап / письмо эксперту).
var has_advisor_pipeline: bool = false

var task_pool: Array[Dictionary] = []
var event_pool: Array[Dictionary] = []
var last_event_index: int = -1
## Следующая неделя без ручного спринта (после выбора в запасном нежданчике).
var sprint_autoplay_mode: String = ""
## Обида друга (отказ отдать рули) — для будущих событий про долю.
var friend_resentment: int = 0
## Первая успешная прожимка цепочки (task_chain) — для бонуса на более позднем этапе.
var task_chain_first_completed_stage: Dictionary = {}

func _init() -> void:
	task_pool = _default_tasks()
	event_pool = _default_events()

func get_runway_months() -> float:
	return cash / max(burn_rate, 1.0)

func get_metrics_text() -> String:
	return "[WEEK] %d / %d\n[CASH] %.0f ₽\n[RUNWAY] %.1f мес\n[MRR] %.0f ₽/мес\n[MORALE] %.0f\n[DEBT] %.0f\n[ACTIVATION] %.0f\n[RETENTION] %.0f\n[REPUTATION] %.0f\n[TEAM] %s\n[PROBLEM FIT] CustDev %.0f / Problem %.0f / MVP %.0f\n[RESEARCH] %.1f\n[MARKET] %s" % [
		week, TOTAL_WEEKS, cash, get_runway_months(), revenue, morale, tech_debt, activation, retention, reputation,
		get_team_summary(), custdev_points, problem_clarity, mvp_progress, research_memory, "В рынке" if launched_to_market else "Не запущен"
	]

func get_nonfounder_share_percent() -> float:
	return clamp(100.0 - founder_share_percent, 0.0, 100.0)


## Строки для блока «Каналы привлечения» в пульсе (видно с CMO).
func get_acquisition_channels_pulse_lines() -> PackedStringArray:
	var lines: PackedStringArray = []
	if not has_marketer:
		return lines
	if not launched_to_market:
		lines.append("До релиза: фокус на посадочной, репутации и ранних тестах; paid-микс ограничен и дорог.")
		return lines
	var stage_key: String = current_stage_key()
	if stage_key == "fit":
		lines.append("Этап Fit: ищем повторяемость креативов и окупаемость CAC, а не сырой охват.")
	elif stage_key == "scale":
		lines.append("Этап Scale: бюджетные миксы, LTV/CAC, частотные капы, сегменты и контроль маржи.")
	if has_growth_agency_contract:
		lines.append("Активен контракт с performance/growth-агентством (возможны нежданчики со счетами).")
	if has_influencer_channel:
		lines.append("Включён канал спонсорств/инфлюенсеров и нишевых медиа.")
	if not has_growth_agency_contract and not has_influencer_channel:
		lines.append("Внешнее агентство не подключено: каналы крутит команда — смотри задачи спринта.")
	return lines


## Детализация выручки для пульса при наличии CCO и продукта в рынке.
func get_revenue_detail_pulse_lines() -> PackedStringArray:
	var out: PackedStringArray = []
	if not has_cco or not launched_to_market:
		return out
	var paying_approx: int = int(max(5.0, round(revenue / 420.0)))
	var arpa: float = revenue / float(paying_approx) if revenue > 0.09 else 0.0
	var nrr_proxy: float = clampf(92.0 + (retention - 28.0) * 0.45, 70.0, 130.0)
	out.append("ARPA (оценка): %.0f ₽/мес при ~%d платящих (игровая модель)." % [arpa, paying_approx])
	out.append("NRR (прокси от удержания): %.0f%% — для тренда, не как GAAP-отчёт." % nrr_proxy)
	out.append("Фокус CCO: отток, апгрейды тарифов, скидки и «качество» выручки, не только MRR.")
	return out


## Подсказка CMO на карточке для задач по каналам (Fit/Scale).
func get_cmo_channel_note_for_task(task: Dictionary) -> String:
	var n: String = str(task.get("name", ""))
	if CMO_CHANNEL_NOTES.has(n):
		return str(CMO_CHANNEL_NOTES[n])
	return str(task.get("cmo_channel_note", ""))


func get_office_summary() -> String:
	match office_tier:
		2:
			return "Класс A"
		1:
			return "Класс B"
		_:
			return "Нет офиса"


func _is_clickable_only_prototype() -> bool:
	return has_playable_prototype and not has_technical_mvp


## Продукт можно вывести в рынок (Build→Fit): есть технический MVP или MVP≥7 не на одном только кликабельном макете.
func product_allows_market_launch() -> bool:
	if has_technical_mvp:
		return true
	if mvp_progress + 0.001 >= MVP_ASSEMBLY_TASK_HIDE_FROM and not _is_clickable_only_prototype():
		return true
	return false


## Уверенный запуск: сильнее стартовые метрики при переходе в рынок.
func launch_prerequisites_met_recommended() -> bool:
	return (
		has_marketer
		and has_cto
		and custdev_points >= 6.0
		and problem_clarity >= 6.0
		and mvp_progress >= MVP_ASSEMBLY_TASK_HIDE_FROM
	)


func get_launch_block_reason_text() -> String:
	if not product_allows_market_launch():
		if _is_clickable_only_prototype():
			return (
				"Выход на рынок закрыт: один только кликабельный макет без бэка — мало. Сделайте «%s» или наберите MVP≥%.0f иначе."
				% [TASK_TECHNICAL_MVP, MVP_ASSEMBLY_TASK_HIDE_FROM]
			)
		return (
			"Выход на рынок: нужен «%s» или прогресс MVP≥%.0f не на одном только кликабельном прототипе."
			% [TASK_TECHNICAL_MVP, MVP_ASSEMBLY_TASK_HIDE_FROM]
		)
	if launch_prerequisites_met_recommended():
		return "Уверенный запуск: CMO, CTO, CustDev≥6, Problem≥6, MVP≥7 — сильнее старт в рынке."
	var parts: Array[String] = []
	if not has_marketer:
		parts.append("CMO")
	if not has_cto:
		parts.append("CTO")
	if custdev_points + 0.001 < 6.0:
		parts.append("CustDev≥6")
	if problem_clarity + 0.001 < 6.0:
		parts.append("Problem≥6")
	if mvp_progress + 0.001 < 7.0:
		parts.append("MVP≥7")
	if parts.is_empty():
		return "Можно выйти в рынок; запуск будет ранним — слабее старт и риск по морали/техдолгу."
	return "Запуск возможен, но ранний — не хватает: " + ", ".join(parts) + "."


## Тот же эффект, что у задачи «Вывести продукт на рынок», но без занятия слота спринта.
func apply_immediate_market_launch() -> String:
	if launched_to_market:
		return "Уже в рынке."
	if not product_allows_market_launch():
		return get_launch_block_reason_text()
	var strict := launch_prerequisites_met_recommended()
	## Как в карточке задачи: сначала переход в Fit, затем бонус к репутации и кассе.
	_apply_effect("launch_market", 1.0)
	_apply_effect("reputation", 2.0)
	_apply_effect("cash", -1200.0)
	if strict:
		return "Готово: продукт выведен на рынок (−1200 ₽, сильный старт по активации/удержанию)."
	return "Выход выполнен (−1200 ₽), но запуск был ранним: команда и продукт не дотянули до «зелёного» чеклиста — слабее метрики и выше техдолг."


func _needs_emergency_funding_offer() -> bool:
	return get_runway_months() < 2.5 or cash < 30000.0


func _stage_order(stage_key: String) -> int:
	match stage_key:
		"build":
			return 0
		"fit":
			return 1
		"scale":
			return 2
		_:
			return 0


func current_stage_key() -> String:
	return str(get_stage_info().get("key", "build"))


func _synergy_name_multisets_equal(names_a: Array, names_b: Array[String]) -> bool:
	if names_a.size() != names_b.size():
		return false
	var counts := {}
	for x in names_a:
		var k: String = str(x)
		counts[k] = int(counts.get(k, 0)) + 1
	for x in names_b:
		var k: String = str(x)
		if not counts.has(k):
			return false
		counts[k] = int(counts[k]) - 1
		if int(counts[k]) < 0:
			return false
	for k in counts:
		if int(counts[k]) != 0:
			return false
	return true


func _synergy_name_in_selected(other: String, selected_names: Array[String]) -> bool:
	for n in selected_names:
		if str(n) == other:
			return true
	return false


func _synergy_canonical_task_name(n: String) -> String:
	if n == TASK_CLICKABLE_PROTOTYPE_REBUILD:
		return TASK_CLICKABLE_PROTOTYPE_ASSEMBLY
	return n


func _synergy_canonical_selected(selected_names: Array[String]) -> Array[String]:
	var out: Array[String] = []
	for n in selected_names:
		out.append(_synergy_canonical_task_name(str(n)))
	return out


func _synergy_selected_has_canonical(target: String, selected_names: Array[String]) -> bool:
	var tt: String = _synergy_canonical_task_name(target)
	for n in selected_names:
		if _synergy_canonical_task_name(str(n)) == tt:
			return true
	return false


## Возвращает "none" | "pair" | "triple" для подсветки карточки в слоте (Build / Fit).
func synergy_visual_for_sprint_task(task_name: String, selected_names: Array[String]) -> String:
	var sk: String = current_stage_key()
	if sk != "build" and sk != "fit":
		return "none"
	if selected_names.size() < 2:
		return "none"
	var tname: String = str(task_name)
	if sk == "fit":
		if selected_names.size() == 3:
			for triple in SYNERGY_TRIPLES_FIT:
				if _synergy_name_multisets_equal(triple, selected_names):
					for x in triple:
						if str(x) == tname:
							return "triple"
					break
		for pair in SYNERGY_PAIRS_FIT:
			var fa: String = str(pair[0])
			var fb: String = str(pair[1])
			if tname == fa and _synergy_name_in_selected(fb, selected_names):
				return "pair"
			if tname == fb and _synergy_name_in_selected(fa, selected_names):
				return "pair"
		return "none"
	var ctname: String = _synergy_canonical_task_name(tname)
	if selected_names.size() == 3:
		var sel_c: Array[String] = _synergy_canonical_selected(selected_names)
		for triple in SYNERGY_TRIPLES_BUILD:
			var tr_c: Array[String] = []
			for x in triple:
				tr_c.append(_synergy_canonical_task_name(str(x)))
			if _synergy_name_multisets_equal(tr_c, sel_c):
				for x in triple:
					if ctname == _synergy_canonical_task_name(str(x)):
						return "triple"
				break
	for pair in SYNERGY_PAIRS_BUILD:
		var a: String = str(pair[0])
		var b: String = str(pair[1])
		if ctname == _synergy_canonical_task_name(a) and _synergy_selected_has_canonical(b, selected_names):
			return "pair"
		if ctname == _synergy_canonical_task_name(b) and _synergy_selected_has_canonical(a, selected_names):
			return "pair"
	return "none"


func task_repeat_chain_multiplier(task: Dictionary) -> float:
	var chain: String = str(task.get("task_chain", ""))
	if chain.is_empty():
		return 1.0
	if not task_chain_first_completed_stage.has(chain):
		return 1.0
	var first_st: String = str(task_chain_first_completed_stage[chain])
	var cur_st: String = current_stage_key()
	if _stage_order(cur_st) <= _stage_order(first_st):
		return 1.0
	return REPEAT_CHAIN_STAGE_MULT


func task_repeat_chain_bonus_will_apply(task: Dictionary) -> bool:
	return task_repeat_chain_multiplier(task) > 1.001


func _scale_effect_delta_for_chain(task: Dictionary, effect_key: String, raw: float) -> float:
	var m: float = task_repeat_chain_multiplier(task)
	if m <= 1.001:
		return raw
	if not _effect_key_is_chain_scalable(effect_key):
		return raw
	return raw * m


func _effect_key_is_chain_scalable(effect_key: String) -> bool:
	for k: String in CHAIN_SCALABLE_EFFECT_KEYS:
		if k == effect_key:
			return true
	return false


func _task_is_product_analytics(task: Dictionary) -> bool:
	return str(task.get("name", "")) == TASK_PRODUCT_ANALYTICS


## Аналитика без тех. MVP (только кликабельный прототип): только cash; с бэкендом или уже в рынке — полный эффект.
func _resolve_product_analytics_effects(task: Dictionary, technical_mvp_ok: bool) -> Dictionary:
	if not _task_is_product_analytics(task):
		return task["effects"] as Dictionary
	var full: Dictionary = (task["effects"] as Dictionary).duplicate(true)
	if launched_to_market or technical_mvp_ok:
		return full
	return {"cash": float(full.get("cash", -700))}


func describe_task_effects_for_ui(task: Dictionary) -> String:
	var eff: Dictionary = _resolve_product_analytics_effects(task, has_technical_mvp)
	var scaled: Dictionary = {}
	for k: Variant in eff.keys():
		var ks: String = str(k)
		scaled[ks] = _scale_effect_delta_for_chain(task, ks, float(eff[k]))
	var base: String = describe_effects(scaled)
	if _task_is_product_analytics(task) and eff.size() == 1:
		return "%s\n(Без технического MVP — только расход; после «%s» или в рынке — полный эффект.)" % [base, TASK_TECHNICAL_MVP]
	return base


func _register_task_chain_completion(task: Dictionary) -> void:
	var chain: String = str(task.get("task_chain", ""))
	if chain.is_empty():
		return
	if task_chain_first_completed_stage.has(chain):
		return
	task_chain_first_completed_stage[chain] = current_stage_key()


func get_budget_breakdown_text() -> String:
	var team_cost: float = 0.0
	var roles: Array[String] = []
	var friends_cost: float = 0.0
	if has_marketer:
		team_cost += CMO_BURN
		roles.append("CMO")
	if has_cto:
		team_cost += CTO_BURN
		roles.append("CTO")
	if has_cfo:
		team_cost += CFO_BURN
		roles.append("CFO")
	if has_hr:
		team_cost += HR_BURN
		roles.append("HR")
	if has_coo:
		team_cost += COO_BURN
		roles.append("COO")
	if has_cco:
		team_cost += CCO_BURN
		roles.append("CCO")
	if has_cpo:
		team_cost += CPO_BURN
		roles.append("CPO")
	if friend_count > 0:
		friends_cost = FRIEND_BURN * float(friend_count)
		roles.append("Друзья x%d" % friend_count)
		team_cost += friends_cost
	var other: float = burn_rate - BASE_BURN - team_cost
	return "Бюджет: база %.0f ₽ | команда %.0f ₽ (%s) | прочее %.0f ₽ | burn %.0f ₽/мес" % [
		BASE_BURN, team_cost, ", ".join(roles) if not roles.is_empty() else "—", other, burn_rate
	]

func get_weekly_tasks() -> Array[Dictionary]:
	var available: Array[Dictionary] = []
	var stage_key: String = str(get_stage_info().get("key", "scale"))
	var stage_fallback: Array[Dictionary] = []
	for task in task_pool:
		if bool(task.get("crisis_only", false)):
			continue
		var task_stage: String = str(task.get("stage", ""))
		# Строго показываем задачи текущего этапа.
		if task_stage != stage_key and task_stage != "all":
			continue
		if has_marketer and task.get("effects", {}).has("hire_marketer"):
			continue
		if has_cto and task.get("effects", {}).has("hire_cto"):
			continue
		if has_cfo and task.get("effects", {}).has("hire_cfo"):
			continue
		if has_hr and task.get("effects", {}).has("hire_hr"):
			continue
		if has_coo and task.get("effects", {}).has("hire_coo"):
			continue
		if has_cco and task.get("effects", {}).has("hire_cco"):
			continue
		if has_cpo and task.get("effects", {}).has("hire_cpo"):
			continue
		# "Startup Fellows" — одноразовый онбординг команды.
		if startup_fellows_enabled and task.get("effects", {}).has("startup_fellows_onboard"):
			continue
		# Жесткий гейт: до запуска Startup Fellows командные задачи закрыты,
		# кроме найма HR и самой задачи Startup Fellows.
		var eff: Dictionary = task.get("effects", {})
		var is_team_task: bool = (
			eff.has("hire_marketer") or eff.has("hire_cto") or eff.has("hire_cfo") or eff.has("hire_coo") or eff.has("hire_cco") or eff.has("hire_cpo") or eff.has("hire_friend")
			or eff.has("fire_marketer") or eff.has("fire_cto") or eff.has("fire_cfo") or eff.has("fire_coo") or eff.has("fire_cco") or eff.has("fire_cpo") or eff.has("fire_friend")
			or eff.has("fire_hr")
		)
		if not startup_fellows_enabled and is_team_task and not eff.has("hire_hr") and not eff.has("startup_fellows_onboard"):
			continue
		if (not has_hr) and (
			task.get("effects", {}).has("hire_marketer")
			or task.get("effects", {}).has("hire_cto")
			or task.get("effects", {}).has("hire_cfo")
			or task.get("effects", {}).has("hire_coo")
			or task.get("effects", {}).has("hire_cco")
			or task.get("effects", {}).has("hire_cpo")
		):
			continue
		if not has_marketer and task.get("effects", {}).has("fire_marketer"):
			continue
		if not has_cto and task.get("effects", {}).has("fire_cto"):
			continue
		if not has_cfo and task.get("effects", {}).has("fire_cfo"):
			continue
		if not has_hr and task.get("effects", {}).has("fire_hr"):
			continue
		if not has_coo and task.get("effects", {}).has("fire_coo"):
			continue
		if not has_cco and task.get("effects", {}).has("fire_cco"):
			continue
		if not has_cpo and task.get("effects", {}).has("fire_cpo"):
			continue
		if friend_count <= 0 and task.get("effects", {}).has("fire_friend"):
			continue
		if office_tier >= 1 and task.get("effects", {}).has("rent_office_b"):
			continue
		if office_tier >= 2 and task.get("effects", {}).has("rent_office_a"):
			continue
		if launched_to_market and task.get("effects", {}).has("launch_market"):
			continue
		if task.get("effects", {}).has("launch_market") and not product_allows_market_launch():
			continue
		if task.get("effects", {}).has("startup_telegram_channel") and has_startup_telegram_channel:
			continue
		if task.has("omit_if_mvp_gte"):
			if mvp_progress + 0.001 >= float(task["omit_if_mvp_gte"]):
				continue
		if bool(task.get("hide_after_first_mvp", false)):
			if has_playable_prototype or has_technical_mvp:
				continue
		if not _task_allowed_by_world_state(task):
			continue
		var tnm: String = str(task.get("name", ""))
		if tnm == TASK_PRODUCT_LANDING and has_product_landing:
			continue
		if tnm == TASK_PRODUCT_LANDING_REDO:
			if not has_product_landing:
				continue
			if not (custdev_points > landing_copy_cd + 0.05 or problem_clarity > landing_copy_pr + 0.05):
				continue
		if tnm == TASK_PRODUCT_ANALYTICS and not has_playable_prototype:
			continue
		available.append(task)
		stage_fallback.append(task)
	available.shuffle()
	if available.size() >= 5:
		return _finalize_weekly_task_pick(stage_key, available.slice(0, 5))
	# Fallback внутри того же этапа (без жестких ограничений), чтобы всегда было 5 опций.
	stage_fallback.shuffle()
	var seen: Dictionary = {}
	for t in available:
		seen[t.get("name", "")] = true
	for t in stage_fallback:
		var name: String = str(t.get("name", ""))
		if not seen.has(name):
			available.append(t)
			seen[name] = true
		if available.size() >= 5:
			break
	return _finalize_weekly_task_pick(stage_key, available.slice(0, min(5, available.size())))


func _finalize_weekly_task_pick(stage_key: String, picked: Array[Dictionary]) -> Array[Dictionary]:
	var p: Array[Dictionary] = picked
	p = _ensure_grant_report_task(stage_key, p)
	p = _ensure_hr_hire_offer(stage_key, p)
	p = _ensure_startup_fellows_in_build(stage_key, p)
	p = _ensure_build_chain_starters(stage_key, p)
	p = _ensure_launch_task_in_build(stage_key, p)
	p = _ensure_emergency_funding_task(stage_key, p)
	return _materialize_tasks_for_current_stage(p)

func _ensure_hr_hire_offer(stage_key: String, picked: Array[Dictionary]) -> Array[Dictionary]:
	if has_hr:
		return picked
	# Почти всегда: пропускаем только в редкую "тихую" неделю.
	var should_force_this_week: bool = (week % 6) != 0
	if not should_force_this_week:
		return picked
	var hr_task: Dictionary = _find_task_by_name("Нанять HR в команду (найм и культура)")
	if hr_task.is_empty():
		return picked
	# Сохраняем stage-уместность, чтобы не тянуть build-задачу в fit/scale.
	var hr_stage: String = str(hr_task.get("stage", ""))
	if hr_stage != "" and hr_stage != "all" and hr_stage != stage_key:
		return picked
	for t in picked:
		if t.get("name", "") == hr_task.get("name", ""):
			return picked
	if picked.size() < 5:
		picked.append(hr_task)
		return picked
	# HR полезно держать в начале списка.
	picked[0] = hr_task
	return picked

func _redo_name_for_task(base_name: String) -> String:
	if base_name.begins_with("Сделать "):
		return "Переделать " + base_name.substr("Сделать ".length())
	if base_name.begins_with("Собрать "):
		return "Пересобрать " + base_name.substr("Собрать ".length())
	if base_name.begins_with("Провести "):
		return "Повторно провести " + base_name.substr("Провести ".length())
	if base_name.begins_with("Запустить "):
		return "Перезапустить " + base_name.substr("Запустить ".length())
	if base_name.begins_with("Подключить "):
		return "Переподключить " + base_name.substr("Подключить ".length())
	return "Переделать: " + base_name

## Возвращаем копии задач с stage-aware названием:
## если цепочка уже была успешно сделана на более раннем этапе, в названии показываем «переделать».
func _materialize_tasks_for_current_stage(picked: Array[Dictionary]) -> Array[Dictionary]:
	var out: Array[Dictionary] = []
	for t in picked:
		var td: Dictionary = (t as Dictionary).duplicate(true)
		var raw_name: String = str(td.get("name", ""))
		if raw_name == TASK_CLICKABLE_PROTOTYPE_ASSEMBLY and completed_assemble_clickable_mvp_task_once:
			td["name"] = TASK_CLICKABLE_PROTOTYPE_REBUILD
		elif task_repeat_chain_bonus_will_apply(td):
			var base_name: String = str(td.get("name", ""))
			td["name"] = _redo_name_for_task(base_name)
		out.append(td)
	return out

func _ensure_startup_fellows_in_build(stage_key: String, picked: Array[Dictionary]) -> Array[Dictionary]:
	if stage_key != "build" or startup_fellows_enabled:
		return picked
	var fellows_task: Dictionary = _find_task_by_name("Найм сотрудников на Startup Fellows")
	if fellows_task.is_empty():
		return picked
	for t in picked:
		if t.get("name", "") == fellows_task.get("name"):
			return picked
	if picked.size() < 5:
		picked.append(fellows_task)
		return picked
	picked[0] = fellows_task
	return picked

func _ensure_build_chain_starters(stage_key: String, picked: Array[Dictionary]) -> Array[Dictionary]:
	if stage_key != "build":
		return picked
	var desk: Dictionary = {}
	for t in task_pool:
		if str(t.get("name", "")) == "Desk research: черновик ICP и сегменты":
			desk = t
			break
	if desk.is_empty():
		return picked
	for t in picked:
		if t.get("name", "") == desk.get("name"):
			return picked
	if picked.size() < 5:
		picked.append(desk)
		return picked
	picked[picked.size() - 1] = desk
	return picked


func _find_task_by_name(task_name: String) -> Dictionary:
	for t in task_pool:
		if str(t.get("name", "")) == task_name:
			return t
	return {}


func _ensure_launch_task_in_build(stage_key: String, picked: Array[Dictionary]) -> Array[Dictionary]:
	if stage_key != "build" or launched_to_market or not product_allows_market_launch():
		return picked
	var launch_task: Dictionary = _find_task_by_name("Вывести продукт на рынок (первый релиз)")
	if launch_task.is_empty():
		return picked
	for t in picked:
		if t.get("name", "") == launch_task.get("name"):
			return picked
	if picked.is_empty():
		return picked
	picked[0] = launch_task
	return picked


func _ensure_emergency_funding_task(_stage_key: String, picked: Array[Dictionary]) -> Array[Dictionary]:
	if not _needs_emergency_funding_offer():
		return picked
	var bridge: Dictionary = _find_task_by_name("Срочно: бридж-раунд (деньги vs доля)")
	if bridge.is_empty():
		return picked
	for t in picked:
		if t.get("name", "") == bridge.get("name"):
			return picked
	if picked.size() < 5:
		picked.append(bridge)
		return picked
	# Второй слот ― заметнее рядом с запуском/бэклогом
	picked[min(1, picked.size() - 1)] = bridge
	return picked


func _grant_report_task_name() -> String:
	var amt: int = int(round(maxf(0.0, grant_report_amount)))
	if grant_report_retry_count > 0:
		return "Повторно сдать отчетность по гранту (%d ₽)" % amt
	return "Сдать отчетность по гранту (%d ₽)" % amt


func _grant_report_task_dict() -> Dictionary:
	return {
		"name": _grant_report_task_name(),
		"stage": "all",
		"effects": {"grant_report_submit": grant_report_amount},
	}


func _task_is_grant_report(task: Dictionary) -> bool:
	return (task.get("effects", {}) as Dictionary).has("grant_report_submit")


func grant_report_required_for_sprint() -> bool:
	if pending_grant_report_review:
		return false
	if not grant_report_required:
		return false
	var sk: String = current_stage_key()
	return sk == "fit" or sk == "scale"


func sprint_has_required_grant_report(selected: Array[Dictionary]) -> bool:
	if not grant_report_required_for_sprint():
		return true
	for t in selected:
		if _task_is_grant_report(t):
			return true
	return false


func _ensure_grant_report_task(stage_key: String, picked: Array[Dictionary]) -> Array[Dictionary]:
	if not grant_report_required:
		return picked
	if pending_grant_report_review:
		return picked
	if stage_key != "fit" and stage_key != "scale":
		return picked
	for t in picked:
		if _task_is_grant_report(t):
			return picked
	var rep: Dictionary = _grant_report_task_dict()
	if picked.size() < 5:
		picked.append(rep)
		return picked
	picked[0] = rep
	return picked


func _task_allowed_by_world_state(task: Dictionary) -> bool:
	var n: String = str(task.get("name", ""))
	if n in [TASK_GRANT_KPI_PACK, TASK_GRANT_STATUS_SYNC, TASK_GRANT_FINAL_AUDIT]:
		if grant_wins_count <= 0 and grant_report_amount <= 0.0 and not grant_report_required:
			return false
	if n in [TASK_ADVISOR_FIT, TASK_ADVISOR_SCALE] and not has_advisor_pipeline:
		return false
	if n in [TASK_OFFICE_FIT, TASK_OFFICE_SCALE] and office_tier <= 0:
		return false
	if n == TASK_LEGAL_FIT and not has_product_landing:
		return false
	if n in [TASK_COMPLIANCE_SCALE, TASK_PARTNER_SCALE] and not launched_to_market:
		return false
	if n == TASK_PARTNER_FIT and reputation < 24.0:
		return false
	return true


func pick_event() -> Dictionary:
	if not should_roll_event_this_week():
		last_event_index = -1
		return {}
	if pending_grant_report_review:
		last_event_index = -1
		return _build_grant_report_verdict_event()
	if pending_grant_review:
		last_event_index = -1
		return _build_grant_verdict_event()
	if pending_accelerator_review:
		last_event_index = -1
		return _build_accelerator_verdict_event()
	var idx: int = _pick_event_index_by_stage()
	if idx < 0:
		idx = _pick_fallback_nezhdan_index()
	if idx < 0:
		last_event_index = -1
		return {}
	last_event_index = idx
	return event_pool[idx]


func _product_strength_01() -> float:
	return clampf((custdev_points + problem_clarity + mvp_progress) / 60.0, 0.0, 1.0)


func _grant_win_probability() -> float:
	## Сильнее продукт (CustDev/Problem/MVP) — выше шанс гранта без доли.
	var p: float = clampf(0.12 + 0.78 * _product_strength_01(), 0.08, 0.93)
	if grant_wins_count <= 0:
		return p
	## Уже получали грант: повторное одобрение сильно реже, «разжимается» только при очень высоком продукте.
	var s: float = _product_strength_01()
	var repeat_curve: float = 0.04 + 0.96 * pow(s, 3.6)
	if grant_wins_count >= 2:
		## Третий и далее гранты — ещё жёстче.
		repeat_curve /= 1.0 + 0.45 * float(grant_wins_count - 1)
	return clampf(p * repeat_curve, 0.02, 0.90)


func _grant_report_success_probability() -> float:
	## Успех отчётности зависит от зрелости продукта и растёт с каждой повторной попыткой.
	var product_core: float = _product_strength_01() # CustDev/Problem/MVP
	var market_signal: float = clampf((activation + retention) / 200.0, 0.0, 1.0)
	var retry_bonus: float = minf(0.28, 0.10 * float(grant_report_retry_count))
	var p: float = 0.30 + 0.36 * product_core + 0.24 * market_signal + retry_bonus
	return clampf(p, 0.18, 0.95)


func _build_grant_report_verdict_event() -> Dictionary:
	var sk: String = str(get_stage_info().get("key", "fit"))
	var ok: bool = randf() < _grant_report_success_probability()
	if ok:
		return {
			"script": "grant_report_verdict",
			"type": "Финансы",
			"stage": sk,
			"grant_report_success": true,
			"title": "Отчётность по гранту принята: грантодатель претензий не имеет.",
			"options": [
				{
					"text": "Фиксируем процесс отчётности и отправляем команде шаблон",
					"effects": {"reputation": 2.0, "morale": 2.0},
				},
				{
					"text": "Тихо закрываем хвост и возвращаемся к PMF-задачам",
					"effects": {"reputation": 1.0, "problem_clarity": 1.0},
				},
			],
		}
	return {
		"script": "grant_report_verdict",
		"type": "Финансы",
		"stage": sk,
		"grant_report_success": false,
		"title": "Отчётность по гранту не принята: нашли замечания по документам и метрикам.",
		"options": [
			{
				"text": "Исправляем пакет документов и пересобираем доказательства",
				"effects": {"morale": -2.0, "tech_debt": 1.0},
			},
			{
				"text": "Привлекаем помощь и готовим повторную сдачу",
				"effects": {"cash": -900.0, "reputation": -1.0, "problem_clarity": 1.0},
			},
		],
	}


func _accelerator_accept_probability() -> float:
	## Слабее продукт — выше шанс попасть в акселератор (как «спасательный» набор).
	return clampf(0.88 - 0.72 * _product_strength_01(), 0.1, 0.9)


func _build_grant_verdict_event() -> Dictionary:
	var sk: String = str(get_stage_info().get("key", "build"))
	var win: bool = randf() < _grant_win_probability()
	var amt: int = int(round(float(pending_grant_amount) * GRANT_WIN_CASH_MULTIPLIER))
	if win:
		return {
			"script": "grant_verdict",
			"type": "Финансы",
			"stage": sk,
			"grant_awarded": true,
			"grant_award_amount": amt,
			"title": "Грант: одобрение — деньги без доли, на счёт компании.",
			"options": [
				{
					"text": "Празднуем открыто: команда видит результат",
					"effects": {"cash": float(amt), "morale": 6.0, "reputation": 2.0},
				},
				{
					"text": "Тихо закрепляем и усиливаем продукт",
					"effects": {"cash": float(amt), "morale": 3.0, "custdev_points": 1.0, "problem_clarity": 1.0},
				},
			],
		}
	return {
		"script": "grant_verdict",
		"type": "Финансы",
		"stage": sk,
		"grant_awarded": false,
		"grant_award_amount": 0,
		"title": "Грант: заявка не прошла — денег нет, настроение под давлением.",
		"options": [
			{
				"text": "Честно обсуждаем с командой и пересобираем план",
				"effects": {"morale": -5.0, "reputation": 1.0},
			},
			{
				"text": "Сохраняем лицо наружу, внутри — жёсткая ретроспектива",
				"effects": {"morale": -3.0, "tech_debt": 1.0},
			},
		],
	}


func _build_accelerator_verdict_event() -> Dictionary:
	var sk: String = str(get_stage_info().get("key", "build"))
	var accept: bool = randf() < _accelerator_accept_probability()
	if accept:
		return {
			"script": "accelerator_verdict",
			"type": "Команда",
			"stage": sk,
			"title": "Акселератор: вас взяли в батч — плотный график, менторы и демо-дни.",
			"options": [
				{
					"text": "Берём максимум менторства (ещё жёстче по времени)",
					"effects": {"accelerator_accept_program": 1.0, "morale": -4.0, "reputation": 3.0},
				},
				{
					"text": "Чуть смягчаем темп, но держим дедлайны",
					"effects": {"accelerator_accept_program": 1.0, "morale": -2.0, "custdev_points": 1.0},
				},
			],
		}
	return {
		"script": "accelerator_verdict",
		"type": "Команда",
		"stage": sk,
		"title": "Акселератор: в этом наборе не прошли — остаётесь на своём темпе.",
		"options": [
			{
				"text": "Фиксируем уроки и ищем другой трек",
				"effects": {"morale": -3.0, "reputation": 1.0},
			},
			{
				"text": "Смиряемся и сжимаем расходы",
				"effects": {"morale": -5.0, "cash": -800.0},
			},
		],
	}

func _prereq_metric_value(metric_key: String, p_cd: float, p_pr: float, p_mv: float, p_rm: float, p_playable: Variant = null, p_landing: Variant = null) -> float:
	match metric_key:
		"custdev_points":
			return p_cd
		"problem_clarity":
			return p_pr
		"mvp_progress":
			return p_mv
		"playable_prototype":
			if p_playable == null:
				return 1.0 if has_playable_prototype else 0.0
			return 1.0 if float(p_playable) + 0.001 >= 1.0 else 0.0
		"product_landing":
			if p_landing == null:
				return 1.0 if has_product_landing else 0.0
			return 1.0 if float(p_landing) + 0.001 >= 1.0 else 0.0
		"research_memory":
			return p_rm
		"activation":
			return activation
		"retention":
			return retention
		_:
			return 0.0


func task_prerequisites_met_with(task: Dictionary, p_cd: float, p_pr: float, p_mv: float, p_rm: float, p_playable: Variant = null, p_landing: Variant = null) -> bool:
	var req: Dictionary = task.get("requires", {}) as Dictionary
	if req.is_empty():
		return true
	for rk: Variant in req.keys():
		var key: String = str(rk)
		var need: float = float(req[rk])
		if _prereq_metric_value(key, p_cd, p_pr, p_mv, p_rm, p_playable, p_landing) + 0.001 < need:
			return false
	return true


func task_prerequisites_met(task: Dictionary) -> bool:
	return task_prerequisites_met_with(task, custdev_points, problem_clarity, mvp_progress, research_memory, null, null)


func describe_prereq_gap(task: Dictionary, p_cd: float, p_pr: float, p_mv: float, p_rm: float, p_playable: Variant = null, p_landing: Variant = null) -> String:
	var req: Dictionary = task.get("requires", {}) as Dictionary
	if req.is_empty():
		return ""
	var parts: Array[String] = []
	for rk: Variant in req.keys():
		var key: String = str(rk)
		var need: float = float(req[rk])
		var cur: float = _prereq_metric_value(key, p_cd, p_pr, p_mv, p_rm, p_playable, p_landing)
		if cur + 0.001 >= need:
			continue
		var label: String = str(PREREQ_METRIC_NAMES.get(key, key))
		parts.append("%s: нужно ≥%.0f, сейчас %.0f" % [label, need, cur])
	if parts.is_empty():
		return ""
	return ". ".join(parts)


func format_requires_hint(task: Dictionary) -> String:
	var req: Dictionary = task.get("requires", {}) as Dictionary
	var bits: Array[String] = []
	for rk: Variant in req.keys():
		var key: String = str(rk)
		var sh: String = key
		match key:
			"custdev_points":
				sh = "CustDev"
			"problem_clarity":
				sh = "Problem"
			"mvp_progress":
				sh = "MVP"
			"playable_prototype":
				sh = "Прототип"
			"product_landing":
				sh = "Лендинг"
			"research_memory":
				sh = "Инсайты"
			_:
				sh = key
		bits.append("%s≥%.0f" % [sh, float(req[rk])])
	var base: String = ""
	if not bits.is_empty():
		base = "База: " + ", ".join(bits)
	if bool(task.get("hide_after_first_mvp", false)):
		if base != "":
			base += " · "
		base += "только до первого MVP (прототип или тех. сборка)"
	return base


func _chain_glyph(ok: bool) -> String:
	return "✓" if ok else "○"


func get_process_chain_title() -> String:
	var sk: String = current_stage_key()
	if sk == "build":
		return "Дорога к релизу (Build)"
	if sk == "fit":
		return "Дорога к PMF и Scale (Fit)"
	return "Дорога к устойчивому росту (Scale)"


func get_process_chain_hint() -> String:
	var sk: String = current_stage_key()
	if sk == "build":
		return "Сегменты → инсайты → прототип → полевые тесты. Дальше — запуск и метрики рынка."
	if sk == "fit":
		return "После релиза считаем продуктовые KPI (активация, удержание, мораль), затем — порог Fit; на Scale фокус на MRR, runway и репутации."
	return "Масштаб: выручка, runway, репутация и «запас прочности» перед концом квартала."
## Подпись под цепочкой в UI (один экран — один смысл этапа).
func get_process_chain_legend_line() -> String:
	return get_process_chain_hint()


func get_process_chain_bbcode(pos_color: String, wait_color: String) -> String:
	var sk: String = current_stage_key()
	if sk == "build":
		return _process_chain_bbcode_build(pos_color, wait_color)
	if sk == "fit":
		return _process_chain_bbcode_fit(pos_color, wait_color)
	return _process_chain_bbcode_scale(pos_color, wait_color)


func _process_chain_bbcode_build(pos_color: String, wait_color: String) -> String:
	var u_seg := custdev_points >= CHAIN_SEGMENTS_NEED_CUSTDEV
	var u_ins := custdev_points >= CHAIN_INSIGHTS_NEED_CUSTDEV or problem_clarity >= CHAIN_INSIGHTS_ALT_PROBLEM
	var u_mvp := has_playable_prototype
	var u_field := has_playable_prototype and problem_clarity >= CHAIN_FIELD_TESTS_NEED_PROBLEM
	var g_seg := _chain_glyph(u_seg)
	var g_ins := _chain_glyph(u_ins)
	var g_mvp := _chain_glyph(u_mvp)
	var g_f := _chain_glyph(u_field)
	var c_seg := pos_color if u_seg else wait_color
	var c_ins := pos_color if u_ins else wait_color
	var c_mvp := pos_color if u_mvp else wait_color
	var c_f := pos_color if u_field else wait_color
	return ("[color=%s]%s[/color] Сегменты  →  [color=%s]%s[/color] Инсайты  →  [color=%s]%s[/color] Прототип  →  [color=%s]%s[/color] Тесты в поле") % [
		c_seg, g_seg, c_ins, g_ins, c_mvp, g_mvp, c_f, g_f
	]


func _process_chain_bbcode_fit(pos_color: String, wait_color: String) -> String:
	var u_act := activation >= FIT_CHAIN_ACTIVATION_MILE
	var u_ret := retention >= FIT_CHAIN_RETENTION_MILE
	var u_mor := morale >= FIT_CHAIN_MORALE_MILE
	var u_pmf := activation >= FIT_CHAIN_ACTIVATION_TARGET and retention >= FIT_CHAIN_RETENTION_TARGET and morale >= FIT_CHAIN_MORALE_TARGET
	var g1 := _chain_glyph(u_act)
	var g2 := _chain_glyph(u_ret)
	var g3 := _chain_glyph(u_mor)
	var g4 := _chain_glyph(u_pmf)
	var c1 := pos_color if u_act else wait_color
	var c2 := pos_color if u_ret else wait_color
	var c3 := pos_color if u_mor else wait_color
	var c4 := pos_color if u_pmf else wait_color
	return ("[color=%s]%s[/color] Активация ≥%.0f (к %.0f)  →  [color=%s]%s[/color] Удержание ≥%.0f (к %.0f)  →  [color=%s]%s[/color] Мораль ≥%.0f (к %.0f)  →  [color=%s]%s[/color] Порог Fit→Scale (все три KPI)") % [
		c1, g1, FIT_CHAIN_ACTIVATION_MILE, FIT_CHAIN_ACTIVATION_TARGET,
		c2, g2, FIT_CHAIN_RETENTION_MILE, FIT_CHAIN_RETENTION_TARGET,
		c3, g3, FIT_CHAIN_MORALE_MILE, FIT_CHAIN_MORALE_TARGET,
		c4, g4,
	]


func _process_chain_bbcode_scale(pos_color: String, wait_color: String) -> String:
	var rw := get_runway_months()
	var u_rev := revenue >= SCALE_CHAIN_REVENUE_MILE
	var u_run := rw >= SCALE_CHAIN_RUNWAY_MIN - 0.05
	var u_rep := reputation >= SCALE_CHAIN_REPUTATION_MILE
	var u_fin := revenue >= SCALE_CHAIN_REVENUE_STRONG and rw >= SCALE_CHAIN_RUNWAY_MIN - 0.05 and reputation >= SCALE_CHAIN_REPUTATION_TARGET
	var g1 := _chain_glyph(u_rev)
	var g2 := _chain_glyph(u_run)
	var g3 := _chain_glyph(u_rep)
	var g4 := _chain_glyph(u_fin)
	var c1 := pos_color if u_rev else wait_color
	var c2 := pos_color if u_run else wait_color
	var c3 := pos_color if u_rep else wait_color
	var c4 := pos_color if u_fin else wait_color
	return ("[color=%s]%s[/color] MRR ≥%.0f ₽ (к %.0f)  →  [color=%s]%s[/color] Runway ≥%.0f мес  →  [color=%s]%s[/color] Репутация ≥%.0f (к %.0f)  →  [color=%s]%s[/color] Цели Scale (три порога сразу)") % [
		c1, g1, SCALE_CHAIN_REVENUE_MILE, SCALE_CHAIN_REVENUE_STRONG,
		c2, g2, SCALE_CHAIN_RUNWAY_MIN,
		c3, g3, SCALE_CHAIN_REPUTATION_MILE, SCALE_CHAIN_REPUTATION_TARGET,
		c4, g4,
	]


func _preview_add_total(totals: Dictionary, effect_key: String, delta: float) -> void:
	if not totals.has(effect_key):
		totals[effect_key] = 0.0
	totals[effect_key] = float(totals[effect_key]) + delta


func _snap_apply_product(key: String, delta: float, p_cd: float, p_pr: float, p_mv: float) -> Array[float]:
	var cd := p_cd
	var pr := p_pr
	var mv := p_mv
	match key:
		"custdev_points":
			cd = clamp(cd + delta, 0.0, 20.0)
		"problem_clarity":
			pr = clamp(pr + delta, 0.0, 20.0)
		"mvp_progress":
			mv = clamp(mv + delta, 0.0, 20.0)
	return [cd, pr, mv]


func _task_commits_landing_page(task: Dictionary) -> bool:
	var n: String = str(task.get("name", ""))
	return n == TASK_PRODUCT_LANDING or n == TASK_PRODUCT_LANDING_REDO


func _commit_landing_page_baseline() -> void:
	has_product_landing = true
	landing_copy_cd = custdev_points
	landing_copy_pr = problem_clarity


func _effects_imply_playable_after_task(effects: Dictionary, task: Dictionary) -> bool:
	for effect_key: Variant in effects.keys():
		var ek: String = str(effect_key)
		var raw_d: float = float(effects[effect_key])
		var d: float = _scale_effect_delta_for_chain(task, ek, raw_d)
		if ek == "playable_prototype" and d > 0.001:
			return true
		if ek == "launch_market" and d > 0.001:
			return true
	return false


## Сумма эффектов выбранных задач с учётом порядка в слотах и отсечения заблокированных задач (как при resolve).
func preview_task_effect_totals(selected_tasks: Array[Dictionary]) -> Dictionary:
	var totals: Dictionary = {
		"cash": 0.0, "revenue": 0.0, "burn_rate": 0.0,
		"activation": 0.0, "retention": 0.0, "morale": 0.0, "tech_debt": 0.0, "reputation": 0.0,
		"custdev_points": 0.0, "problem_clarity": 0.0, "mvp_progress": 0.0,
		"founder_share": 0.0,
	}
	var cd := custdev_points
	var pr := problem_clarity
	var mv := mvp_progress
	var rm := research_memory
	var pp: float = 1.0 if has_playable_prototype else 0.0
	var land: float = 1.0 if has_product_landing else 0.0
	var htm := has_technical_mvp
	for task in selected_tasks:
		if not task_prerequisites_met_with(task, cd, pr, mv, rm, pp, land):
			continue
		var ch_m: float = task_repeat_chain_multiplier(task)
		var ig_raw: float = float(task.get("insight_gain", 0.0))
		var insight_gain: float = ig_raw * ch_m if ig_raw > 0.0 else 0.0
		if insight_gain > 0.0:
			rm = clamp(rm + insight_gain, 0.0, 10.0)
		var effects: Dictionary = _resolve_product_analytics_effects(task, htm)
		for effect_key: Variant in effects.keys():
			var ek: String = str(effect_key)
			var raw_d: float = float(effects[effect_key])
			var d: float = _scale_effect_delta_for_chain(task, ek, raw_d)
			if ek == "grant_apply":
				_preview_add_total(totals, "cash", d * _grant_win_probability() * GRANT_WIN_CASH_MULTIPLIER)
				continue
			if ek == "accelerator_apply":
				continue
			if ek == "growth_agency_contract" or ek == "growth_agency_churn" or ek == "influencer_channel":
				continue
			if ek == "technical_mvp" or ek == "playable_prototype":
				continue
			_preview_add_total(totals, ek, d)
			var sn: Array[float] = _snap_apply_product(ek, d, cd, pr, mv)
			cd = sn[0]
			pr = sn[1]
			mv = sn[2]
		var insight_scaling: Dictionary = task.get("insight_scaling", {})
		if not insight_scaling.is_empty() and rm > 0.0:
			var bonus_mult: float = min(rm, 6.0) / 6.0
			for stat: Variant in insight_scaling.keys():
				var sk: String = str(stat)
				var base_bonus: float = float(insight_scaling[stat]) * ch_m
				var bonus: float = round(base_bonus * bonus_mult)
				if bonus == 0.0:
					continue
				_preview_add_total(totals, sk, bonus)
				var sn2: Array[float] = _snap_apply_product(sk, bonus, cd, pr, mv)
				cd = sn2[0]
				pr = sn2[1]
				mv = sn2[2]
		if _effects_imply_playable_after_task(effects, task):
			pp = 1.0
		if str(task.get("name", "")) == TASK_TECHNICAL_MVP:
			htm = true
		if _task_commits_landing_page(task):
			land = 1.0
	return totals


## Снимок метрик после применения первых `count` задач спринта (порядок слотов 0→1→2). Для подсветки карточек.
func get_snap_after_tasks(selected_tasks: Array[Dictionary], count: int) -> Dictionary:
	var cd := custdev_points
	var pr := problem_clarity
	var mv := mvp_progress
	var rm := research_memory
	var pp: float = 1.0 if has_playable_prototype else 0.0
	var land: float = 1.0 if has_product_landing else 0.0
	var htm := has_technical_mvp
	var n: int = min(count, selected_tasks.size())
	for i in n:
		var task: Dictionary = selected_tasks[i]
		if not task_prerequisites_met_with(task, cd, pr, mv, rm, pp, land):
			continue
		var ch_m: float = task_repeat_chain_multiplier(task)
		var ig_raw: float = float(task.get("insight_gain", 0.0))
		var insight_gain: float = ig_raw * ch_m if ig_raw > 0.0 else 0.0
		if insight_gain > 0.0:
			rm = clamp(rm + insight_gain, 0.0, 10.0)
		var effects: Dictionary = _resolve_product_analytics_effects(task, htm)
		for effect_key: Variant in effects.keys():
			var ek: String = str(effect_key)
			var raw_d: float = float(effects[effect_key])
			var d: float = _scale_effect_delta_for_chain(task, ek, raw_d)
			if ek == "technical_mvp" or ek == "playable_prototype":
				continue
			var sn: Array[float] = _snap_apply_product(ek, d, cd, pr, mv)
			cd = sn[0]
			pr = sn[1]
			mv = sn[2]
		var insight_scaling: Dictionary = task.get("insight_scaling", {})
		if not insight_scaling.is_empty() and rm > 0.0:
			var bonus_mult: float = min(rm, 6.0) / 6.0
			for stat: Variant in insight_scaling.keys():
				var base_bonus: float = float(insight_scaling[stat]) * ch_m
				var bonus: float = round(base_bonus * bonus_mult)
				if bonus == 0.0:
					continue
				var sn2: Array[float] = _snap_apply_product(str(stat), bonus, cd, pr, mv)
				cd = sn2[0]
				pr = sn2[1]
				mv = sn2[2]
		if _effects_imply_playable_after_task(effects, task):
			pp = 1.0
		if str(task.get("name", "")) == TASK_TECHNICAL_MVP:
			htm = true
		if _task_commits_landing_page(task):
			land = 1.0
	return {"cd": cd, "pr": pr, "mv": mv, "rm": rm, "pp": pp, "land": land}


func _trust_crisis_event_weight_for_reputation() -> float:
	var t: float = clampf(reputation / 100.0, 0.0, 1.0)
	return lerpf(TRUST_CRISIS_EVENT_WEIGHT_REP_0, TRUST_CRISIS_EVENT_WEIGHT_REP_100, t)


func _option_effects_imply_trust_crisis(eff: Dictionary) -> bool:
	for k in eff.keys():
		var ks: String = str(k)
		if ks.begins_with("fire_") and float(eff[k]) + 0.001 > 0.0:
			return true
		if ks == "founder_share" and float(eff[k]) + 0.001 < 0.0:
			return true
	return false


func _event_has_trust_crisis_options(ev: Dictionary) -> bool:
	for opt in ev.get("options", []):
		if opt is Dictionary and _option_effects_imply_trust_crisis(opt.get("effects", {}) as Dictionary):
			return true
	return false


func _event_weight_for_trust_crisis_pool(ev: Dictionary) -> float:
	if _event_has_trust_crisis_options(ev):
		return _trust_crisis_event_weight_for_reputation()
	return 1.0


func _pick_random_index_weighted(indices: Array[int]) -> int:
	if indices.is_empty():
		return -1
	if indices.size() == 1:
		return indices[0]
	var weights: Array[float] = []
	var total: float = 0.0
	for idx in indices:
		var w: float = _event_weight_for_trust_crisis_pool(event_pool[idx])
		weights.append(w)
		total += w
	if total <= 0.001:
		return indices[randi_range(0, indices.size() - 1)]
	var r: float = randf() * total
	var acc: float = 0.0
	for j in range(indices.size()):
		acc += weights[j]
		if r <= acc:
			return indices[j]
	return indices[indices.size() - 1]


func _pick_event_index_by_stage() -> int:
	var stage_key: String = str(get_stage_info().get("key", "scale"))
	var stage_events: Array[int] = []
	var same_stage_including_last: Array[int] = []

	for i in event_pool.size():
		var e: Dictionary = event_pool[i]
		if not _event_matches_stage(e, stage_key):
			continue
		if not _event_matches_world_state(e):
			continue
		same_stage_including_last.append(i)
		if i != last_event_index:
			stage_events.append(i)

	# Строго: события этапа + контекст мира (прод / MVP, роли в команде и т.д.).
	var source: Array[int] = stage_events if not stage_events.is_empty() else same_stage_including_last
	if source.is_empty():
		# Нельзя ослаблять фильтр мира: иначе попадут сюжеты «CFO предлагает…» без CFO.
		for i in event_pool.size():
			var e2: Dictionary = event_pool[i]
			if _event_matches_stage(e2, stage_key) and _event_matches_world_state(e2) and i != last_event_index:
				return i
		for i in event_pool.size():
			var e3: Dictionary = event_pool[i]
			if _event_matches_stage(e3, stage_key) and _event_matches_world_state(e3):
				return i
		return -1
	return _pick_random_index_weighted(source)


func _pick_fallback_nezhdan_index() -> int:
	var pool: Array[int] = []
	for i in event_pool.size():
		var e: Dictionary = event_pool[i]
		if not bool(e.get("fallback_nezhdan", false)):
			continue
		if bool(e.get("fallback_needs_friend", false)) and friend_count <= 0:
			continue
		if bool(e.get("fallback_needs_no_friend", false)) and friend_count > 0:
			continue
		if i != last_event_index:
			pool.append(i)
	if pool.is_empty():
		for i in event_pool.size():
			var e2: Dictionary = event_pool[i]
			if not bool(e2.get("fallback_nezhdan", false)):
				continue
			if bool(e2.get("fallback_needs_friend", false)) and friend_count <= 0:
				continue
			if bool(e2.get("fallback_needs_no_friend", false)) and friend_count > 0:
				continue
			pool.append(i)
	if pool.is_empty():
		return -1
	return pool[randi_range(0, pool.size() - 1)]


func _preferred_event_types_for_week() -> Array[String]:
	if week <= 6:
		return ["Продукт", "Команда", "Техника"]
	if week <= 12:
		return ["Рынок", "Продукт", "Техника"]
	return ["Инвестор", "Рынок", "PR/Репутация"]

func _event_matches_stage(event_data: Dictionary, stage_key: String) -> bool:
	# Если у события есть явный stage — доверяем ему.
	var explicit_stage: String = str(event_data.get("stage", ""))
	if explicit_stage != "":
		if explicit_stage == "all":
			return true
		return explicit_stage == stage_key
	# Иначе используем тип события как stage-mapping.
	var event_type: String = str(event_data.get("type", "Разное"))
	if stage_key == "build":
		return event_type in ["Продукт", "Команда", "Техника"]
	if stage_key == "fit":
		return event_type in ["Рынок", "Продукт", "Техника"]
	return event_type in ["Инвестор", "Рынок", "PR/Репутация"]


## Нежданчик должен совпадать с «фазой мира»: нельзя падение прода до выхода в рынок/MVP.
func _event_matches_world_state(event_data: Dictionary) -> bool:
	# Тех-нежданчики только когда уже есть хоть какая-то продуктовая работа.
	if str(event_data.get("type", "")) == "Техника" and mvp_progress + 0.001 < 1.0:
		return false
	if bool(event_data.get("requires_playable_prototype", false)):
		if not has_playable_prototype:
			return false
	if bool(event_data.get("requires_live_product", false)):
		if not launched_to_market:
			return false
	if event_data.has("min_mvp_progress"):
		var need_m: float = float(event_data["min_mvp_progress"])
		if mvp_progress + 0.001 < need_m:
			return false
	if event_data.has("max_mvp_progress"):
		var cap_m: float = float(event_data["max_mvp_progress"])
		if mvp_progress - 0.001 > cap_m:
			return false
	if bool(event_data.get("only_before_launch", false)) and launched_to_market:
		return false
	if bool(event_data.get("requires_any_hire", false)):
		if not has_marketer and not has_cto and not has_cfo and not has_hr and not has_coo and not has_cco and not has_cpo and friend_count <= 0:
			return false
	# События с перечислением ролей в тексте: все перечисленные должны быть в команде.
	if event_data.has("requires_roles"):
		var req_roles: Array = event_data.get("requires_roles", [])
		for rv in req_roles:
			var role: String = str(rv)
			if role == "CMO" and not has_marketer:
				return false
			if role == "CTO" and not has_cto:
				return false
			if role == "CFO" and not has_cfo:
				return false
			if role == "HR" and not has_hr:
				return false
			if role == "COO" and not has_coo:
				return false
			if role == "CCO" and not has_cco:
				return false
			if role == "CPO" and not has_cpo:
				return false
			if role == "FRIEND" and friend_count <= 0:
				return false
	# До Startup Fellows командные нежданчики (кроме HR-исключений по задачам) запрещены.
	var event_type: String = str(event_data.get("type", "Разное"))
	if not startup_fellows_enabled and event_type == "Команда":
		return false
	if bool(event_data.get("requires_friend", false)):
		if friend_count <= 0:
			return false
	if bool(event_data.get("requires_investor_contact", false)):
		if not has_investor_contact:
			return false
	if bool(event_data.get("requires_office", false)):
		if office_tier <= 0:
			return false
	if bool(event_data.get("requires_office_a", false)):
		if office_tier < 2:
			return false
	if bool(event_data.get("requires_growth_agency", false)):
		if not has_growth_agency_contract:
			return false
	if bool(event_data.get("requires_influencer_channel", false)):
		if not has_influencer_channel:
			return false
	if bool(event_data.get("requires_advisor_pipeline", false)):
		if not has_advisor_pipeline:
			return false
	if bool(event_data.get("requires_startup_telegram_channel", false)):
		if not has_startup_telegram_channel:
			return false
	if bool(event_data.get("requires_custdev_activity", false)):
		if not has_custdev_activity:
			return false
	if bool(event_data.get("requires_expert_search_activity", false)):
		if not has_expert_search_activity:
			return false
	if bool(event_data.get("requires_interview_activity", false)):
		if not has_interview_activity:
			return false
	if bool(event_data.get("requires_friend_equity_partner", false)):
		if not has_friend_equity_partner:
			return false
	return true

func apply_tasks(selected_tasks: Array[Dictionary]) -> Array[String]:
	var logs: Array[String] = []
	if cash <= 0.0:
		var took_bridge := false
		for t in selected_tasks:
			if bool((t as Dictionary).get("crisis_only", false)):
				took_bridge = true
				break
		if not took_bridge:
			game_over = true
			game_over_art_key = "bankrupt"
			game_over_reason = "Кассовый разрыв: денег не осталось, а бридж-раунд за долю не взят. Компания не доживает до следующей недели."
			logs.append("⚠ Кассовый разрыв: при cash <= 0 нужно обязательно выбрать «Срочно: бридж-раунд (деньги vs доля)».")
			return logs
	var cd := custdev_points
	var pr := problem_clarity
	var mv := mvp_progress
	var rm := research_memory
	for task in selected_tasks:
		var tname: String = str(task.get("name", ""))
		if not task_prerequisites_met_with(task, cd, pr, mv, rm, null, null):
			logs.append(
				"⚠ «%s»: эффектов нет — %s. Нельзя получить результат этапа без нужной базы (смотри цепочку [PROCESS CHAIN])."
				% [tname, describe_prereq_gap(task, cd, pr, mv, rm, null, null)]
			)
			continue
		if bool(task.get("crisis_only", false)):
			has_investor_contact = true
		var tname_l: String = tname.to_lower()
		if tname_l.contains("custdev") or tname_l.contains("кастдев"):
			has_custdev_activity = true
		if tname_l.contains("интервью"):
			has_interview_activity = true
			has_custdev_activity = true
		if tname_l.contains("эксперт") or tname_l.contains("митап") or tname_l.contains("нетворк"):
			has_expert_search_activity = true
		# Продуктовый блок ачивок: считаем только успешно применённые задачи.
		if tname == "Провести 5 CustDev-интервью":
			stat_custdev_interviews_done += 5
		elif tname == "Провести CustDev (8 интервью)":
			stat_custdev_interviews_done += 8
		elif tname == "CustDev волна 3: 12 глубинных интервью (ICP + enterprise)":
			stat_custdev_interviews_done += 12
		if tname == TASK_FIT_PILOT_SELECT or tname == TASK_FIT_PILOT_SUPPORT or tname == TASK_FIT_PILOT_ITERATE:
			stat_pilot_steps_done += 1
		if tname == "Проверить прототип на 5 пользователях":
			stat_prototype_users_tested += 5
		elif tname == "Провести usability-тесты (5 сессий)":
			stat_prototype_users_tested += 5
		elif tname == "UX-лаборатория: 8 сессий на реальных пользователях":
			stat_prototype_users_tested += 8
		if tname == "10 холодных касаний: кастдев-письма сегменту":
			stat_cold_outreach_count += 10
		if tname == "Провести интервью с отраслевыми экспертами (не пользователи)":
			stat_expert_interviews_count += 1
		var ch_m: float = task_repeat_chain_multiplier(task)
		if ch_m > 1.001:
			logs.append("«%s»: повтор продуктовой цепочки на новом этапе — числовые эффекты +%d%%." % [tname, int(round((ch_m - 1.0) * 100.0))])
		var insight_gain_raw: float = float(task.get("insight_gain", 0.0))
		var insight_gain: float = insight_gain_raw * ch_m if insight_gain_raw > 0.0 else 0.0
		if insight_gain > 0.0:
			research_memory = clamp(research_memory + insight_gain, 0.0, 10.0)
			rm = research_memory
			logs.append("Инсайты +%.1f (всего %.1f): %s" % [insight_gain, research_memory, tname])
		var effects: Dictionary = _resolve_product_analytics_effects(task, has_technical_mvp)
		if _task_is_product_analytics(task) and effects.size() == 1 and effects.has("cash"):
			logs.append(
				"«%s»: без технического MVP аналитика не цепляется к реальным данным — только расход, без роста метрик."
				% tname
			)
		for effect_key: String in effects:
			var raw_d: float = float(effects[effect_key])
			var d: float = _scale_effect_delta_for_chain(task, effect_key, raw_d)
			if effect_key == "grant_apply":
				pending_grant_review = true
				pending_grant_amount = d
				var glog: String = (
					"🧾 Заявка на грант отправлена (%s); вердикт — в пятничном нежданчике (сильный продукт повышает шанс)."
					% tname
				)
				if grant_wins_count >= 1:
					glog += " Повторный грант после уже полученного: одобрение только при очень сильных CustDev/Problem/MVP."
				logs.append(glog)
				continue
			if effect_key == "grant_report_submit":
				pending_grant_report_review = true
				pending_grant_report_amount = maxf(d, grant_report_amount)
				logs.append("🧾 Отчётность по гранту отправлена; результат — в пятничном нежданчике.")
				continue
			if effect_key == "accelerator_apply":
				pending_accelerator_review = true
				logs.append(
					"🚀 Заявка в акселератор отправлена (%s); отбор — в пятничном нежданчике (при слабом продукте шанс выше)." % tname
				)
				continue
			_apply_effect(effect_key, d)
		cd = custdev_points
		pr = problem_clarity
		mv = mvp_progress
		rm = research_memory
		var insight_scaling: Dictionary = task.get("insight_scaling", {})
		if not insight_scaling.is_empty() and research_memory > 0.0:
			var bonus_mult: float = min(research_memory, 6.0) / 6.0
			var bonus_lines: Array[String] = []
			for stat in insight_scaling.keys():
				var base_bonus: float = float(insight_scaling[stat]) * ch_m
				var bonus: float = round(base_bonus * bonus_mult)
				if bonus != 0.0:
					_apply_effect(str(stat), bonus)
					bonus_lines.append("%s %+d" % [str(stat), int(bonus)])
			if not bonus_lines.is_empty():
				logs.append("Синергия инсайтов (%s): %s" % [tname, ", ".join(bonus_lines)])
		cd = custdev_points
		pr = problem_clarity
		mv = mvp_progress
		rm = research_memory
		if _task_commits_landing_page(task):
			_commit_landing_page_baseline()
			cd = custdev_points
			pr = problem_clarity
		_register_task_chain_completion(task)
		if tname == TASK_CLICKABLE_PROTOTYPE_ASSEMBLY:
			completed_assemble_clickable_mvp_task_once = true
		logs.append("Задача: %s" % tname)
	return logs

func apply_event_option(event_data: Dictionary, option_index: int) -> String:
	var script: String = str(event_data.get("script", ""))
	if script == "grant_report_verdict":
		pending_grant_report_review = false
		pending_grant_report_amount = 0.0
		if bool(event_data.get("grant_report_success", false)):
			grant_report_required = false
			grant_report_retry_count = 0
		else:
			grant_report_required = true
			grant_report_retry_count += 1
	if script == "grant_verdict":
		pending_grant_review = false
		pending_grant_amount = 0.0
		if bool(event_data.get("grant_awarded", false)):
			grant_wins_count += 1
			var amt_awarded: float = float(event_data.get("grant_award_amount", 0.0))
			grant_report_amount += maxf(0.0, amt_awarded)
			grant_report_required = grant_report_amount > 0.0
			grant_report_retry_count = 0
	if script == "accelerator_verdict":
		pending_accelerator_review = false
	var options: Array = event_data["options"]
	var option: Dictionary = options[option_index]
	var option_effects: Dictionary = option["effects"]
	for effect_key: String in option_effects:
		if effect_key == "accelerator_accept_program":
			accelerator_program_weeks_left = 3
			has_advisor_pipeline = true
			continue
		if effect_key == "autoplay_vacation" and float(option_effects[effect_key]) > 0.5:
			sprint_autoplay_mode = "vacation_solo"
			continue
		if effect_key == "autoplay_vacation_friend" and float(option_effects[effect_key]) > 0.5:
			sprint_autoplay_mode = "vacation_friend"
			continue
		if effect_key == "autoplay_friend_pilot" and float(option_effects[effect_key]) > 0.5:
			sprint_autoplay_mode = "friend_pilot"
			continue
		if effect_key == "friend_resentment":
			friend_resentment += int(round(float(option_effects[effect_key])))
			continue
		_apply_effect(effect_key, float(option_effects[effect_key]))
	if bool(event_data.get("investor_contact", false)):
		has_investor_contact = true
	var event_type: String = str(event_data.get("type", "Разное"))
	return "Событие [%s]: %s -> %s" % [event_type, str(event_data.get("title", "")), str(option.get("text", ""))]

func should_roll_event_this_week() -> bool:
	# Неделя 1: только обучение механики, без розыгрыша нежданчика.
	if week <= 1:
		return false
	# Со 2-й недели каждую пятницу — нежданчик (варианты A/B).
	return true

func finish_week() -> void:
	last_team_bonus_log = _apply_team_perks()
	cash += revenue - burn_rate
	_track_cash_milestones()
	research_memory = max(0.0, research_memory - 1.0)
	_tick_accelerator_program()
	week += 1
	_check_end_conditions()


func has_sprint_autoplay_pending() -> bool:
	return sprint_autoplay_mode != ""


## Неделя без ручного спринта: штрафы за «автопилот», затем тот же такт кассы, что и в finish_week().
func apply_skipped_sprint_autoplay() -> Array[String]:
	if sprint_autoplay_mode == "":
		return []
	var logs: Array[String] = []
	match sprint_autoplay_mode:
		"vacation_solo":
			cash -= 9200.0
			morale = clamp(morale - 7.0, 0.0, 100.0)
			tech_debt = clamp(tech_debt + 4.0, 0.0, 100.0)
			reputation = clamp(reputation - 2.0, 0.0, 100.0)
			activation = clamp(activation - 2.0, 0.0, 100.0)
			logs.append("✈ Мамкин в отпуске: спринта не было — приоритеты и коммуникация просели.")
		"vacation_friend":
			cash -= 14500.0
			morale = clamp(morale - 6.0, 0.0, 100.0)
			tech_debt = clamp(tech_debt + 6.0, 0.0, 100.0)
			reputation = clamp(reputation - 3.0, 0.0, 100.0)
			retention = clamp(retention - 3.0, 0.0, 100.0)
			activation = clamp(activation - 4.0, 0.0, 100.0)
			logs.append("✈ Мамкин в отпуске, у руля друг: импульсивные траты и сдвиг фокуса в продукте.")
		"friend_pilot":
			cash -= 7200.0
			morale = clamp(morale - 4.0, 0.0, 100.0)
			tech_debt = clamp(tech_debt + 5.0, 0.0, 100.0)
			mvp_progress = clamp(mvp_progress - 1.0, 0.0, 20.0)
			problem_clarity = clamp(problem_clarity - 1.0, 0.0, 20.0)
			reputation = clamp(reputation - 1.0, 0.0, 100.0)
			logs.append("🎮 Неделя на автопилоте под другом: решения без согласований — хаос в backlog и метриках.")
		_:
			logs.append("⚠ Автопилот: неизвестный режим.")
	_track_cash_milestones()
	sprint_autoplay_mode = ""
	finish_week()
	return logs


func _tick_accelerator_program() -> void:
	if accelerator_program_weeks_left <= 0:
		return
	morale = clamp(morale - 2.0, 0.0, 100.0)
	accelerator_program_weeks_left -= 1
	if accelerator_program_weeks_left > 0:
		return
	custdev_points = clamp(custdev_points + 2.0, 0.0, 20.0)
	problem_clarity = clamp(problem_clarity + 2.0, 0.0, 20.0)
	mvp_progress = clamp(mvp_progress + 2.0, 0.0, 20.0)
	reputation = clamp(reputation + 2.0, 0.0, 100.0)
	morale = clamp(morale + 5.0, 0.0, 100.0)
	var bonus_line := "🎓 Акселератор завершён: +2 CustDev/Problem/MVP, +2 репутации, команда выдохнула."
	if last_team_bonus_log.strip_edges() != "":
		last_team_bonus_log = last_team_bonus_log + "\n" + bonus_line
	else:
		last_team_bonus_log = bonus_line

func consume_team_bonus_log() -> String:
	var out := last_team_bonus_log
	last_team_bonus_log = ""
	return out

func _apply_effect(key: String, delta: float) -> void:
	match key:
		"cash":
			cash += delta
			_track_cash_milestones()
		"revenue":
			revenue += delta
		"burn_rate":
			burn_rate += delta
		"morale":
			morale = clamp(morale + delta, 0.0, 100.0)
		"tech_debt":
			tech_debt = clamp(tech_debt + delta, 0.0, 100.0)
		"activation":
			activation = clamp(activation + delta, 0.0, 100.0)
		"retention":
			retention = clamp(retention + delta, 0.0, 100.0)
		"reputation":
			reputation = clamp(reputation + delta, 0.0, 100.0)
		"hire_marketer":
			has_marketer = true
		"hire_cto":
			has_cto = true
		"hire_cfo":
			has_cfo = true
		"hire_hr":
			has_hr = true
		"hire_coo":
			has_coo = true
		"hire_cco":
			has_cco = true
		"hire_cpo":
			has_cpo = true
		"hire_friend":
			friend_count += max(1, int(round(delta)))
			if delta > 0.001:
				has_friend_equity_partner = true
		"startup_fellows_onboard":
			startup_fellows_enabled = true
		"rent_office_b":
			if office_tier == 0:
				office_tier = 1
				burn_rate += OFFICE_B_BURN
			elif office_tier == 2:
				office_tier = 1
				burn_rate = max(6000.0, burn_rate - (OFFICE_A_BURN - OFFICE_B_BURN))
		"rent_office_a":
			if office_tier == 0:
				office_tier = 2
				burn_rate += OFFICE_A_BURN
			elif office_tier == 1:
				office_tier = 2
				burn_rate += (OFFICE_A_BURN - OFFICE_B_BURN)
		"fire_marketer":
			has_marketer = false
		"fire_cto":
			has_cto = false
		"fire_cfo":
			has_cfo = false
		"fire_hr":
			has_hr = false
		"fire_coo":
			has_coo = false
		"fire_cco":
			has_cco = false
		"fire_cpo":
			has_cpo = false
		"fire_friend":
			friend_count = max(0, friend_count - max(1, int(round(delta))))
		"custdev_points":
			custdev_points = clamp(custdev_points + delta, 0.0, 20.0)
		"problem_clarity":
			problem_clarity = clamp(problem_clarity + delta, 0.0, 20.0)
		"mvp_progress":
			mvp_progress = clamp(mvp_progress + delta, 0.0, 20.0)
		"playable_prototype":
			if delta > 0.001:
				has_playable_prototype = true
		"technical_mvp":
			if delta > 0.001:
				has_technical_mvp = true
		"launch_market":
			if delta <= 0.001:
				return
			if not product_allows_market_launch() or launched_to_market:
				return
			launched_to_market = true
			has_playable_prototype = true
			has_technical_mvp = true
			if launch_prerequisites_met_recommended():
				activation = max(activation, 20.0)
				retention = max(retention, 14.0)
				reputation = clamp(reputation + 3.0, 0.0, 100.0)
			else:
				activation = max(activation, 12.0)
				retention = max(retention, 9.0)
				reputation = clamp(reputation - 3.0, 0.0, 100.0)
				morale = clamp(morale - 6.0, 0.0, 100.0)
				tech_debt = clamp(tech_debt + 5.0, 0.0, 100.0)
		"founder_share":
			founder_share_percent = clamp(founder_share_percent + delta, 0.0, 100.0)
			_check_founder_control_loss()
		"growth_agency_contract":
			if delta > 0.01:
				has_growth_agency_contract = true
		"growth_agency_churn":
			has_growth_agency_contract = false
		"influencer_channel":
			if delta > 0.01:
				has_influencer_channel = true
		"startup_telegram_channel":
			if delta > 0.01:
				has_startup_telegram_channel = true
		"startup_telegram_channel_close":
			if delta > 0.01:
				has_startup_telegram_channel = false
		"smoothie_bar":
			if delta > 0.01:
				has_smoothie_bar = true
		"coffee_lady":
			if delta > 0.01:
				has_coffee_lady = true
		"office_masseur":
			if delta > 0.01:
				has_office_masseur = true
		"advisor_pipeline":
			if delta > 0.01:
				has_advisor_pipeline = true


func _track_cash_milestones() -> void:
	if cash < 100000.0:
		has_seen_cash_below_100k = true

func _check_founder_control_loss() -> void:
	if founder_share_percent + 0.001 >= 50.0:
		return
	game_over = true
	game_over_art_key = "dilution"
	game_over_reason = "Совет и ключевая команда проголосовали: у Мамкина меньше 50% компании. Его вежливо, но настойчиво выпроваживают из своего же стартапа."


func _check_end_conditions() -> void:
	if game_over:
		return
	if founder_share_percent + 0.001 < 50.0:
		game_over = true
		game_over_art_key = "dilution"
		game_over_reason = "Совет и ключевая команда проголосовали: у Мамкина меньше 50% компании. Его вежливо, но настойчиво выпроваживают из своего же стартапа."
		return
	if morale <= 5:
		game_over = true
		game_over_art_key = "burnout"
		game_over_reason = "Команда выгорела. Даже ирония больше не спасает."
		return
	if week > TOTAL_WEEKS:
		game_over = true
		if retention >= 35.0 and activation >= 40.0 and cash > 0.0:
			game_over_art_key = "triumph"
			game_over_reason = "Успех! Мамкин почти единорог: метрики и деньги в норме."
		elif cash > 0.0:
			game_over_art_key = "struggle"
			game_over_reason = "Выжили, но PMF еще не пойман. Время на пивот."
		else:
			game_over_art_key = "scold"
			game_over_reason = "Квартал закрыт в минус. Инвестор читает нотации."

func get_team_summary() -> String:
	var arr: Array[String] = []
	if startup_fellows_enabled:
		arr.append("Startup Fellows: активен")
	else:
		arr.append("Startup Fellows: не запущен")
	if has_hr:
		arr.append("HR (найм и культура)")
	if has_marketer:
		arr.append("CMO (маркетинг и рост)")
	if has_cto:
		arr.append("CTO (продукт и разработка)")
	if has_cfo:
		arr.append("CFO (финансы и burn)")
	if has_coo:
		arr.append("COO (операции и процессы)")
	if has_cco:
		arr.append("CCO (продажи и выручка)")
	if has_cpo:
		arr.append("CPO (продукт и метрики)")
	if friend_count > 0:
		arr.append("Друг x%d" % friend_count)
	if friend_resentment > 0:
		arr.append("напряжение с другом %d" % friend_resentment)
	if arr.is_empty():
		return "команда не собрана"
	return ", ".join(arr)

func get_stage_info() -> Dictionary:
	if not launched_to_market:
		var goal: String = "Собрать команду, провести CustDev, доказать проблему и собрать MVP."
		if product_allows_market_launch():
			if launch_prerequisites_met_recommended():
				goal = "Продукт можно вывести в рынок: задача «Вывести продукт на рынок (первый релиз)» или кнопка под спринтом — переход к этапу Fit."
			else:
				goal = "Выход на рынок уже доступен (задача или кнопка), но чеклист неполный — запуск будет «ранним» (слабее метрики, выше техдолг). Сильнее старт при CMO+CTO и CustDev/Problem/MVP≥6/6/7."
		elif mvp_progress >= MVP_ASSEMBLY_TASK_HIDE_FROM:
			goal = "MVP по цифрам на уровне релиза, но для выхода в рынок нужен технический MVP или прогресс не на одном только кликабельном макете."
		return {
			"key": "build",
			"name": "Build (до выхода в рынок)",
			"goal": goal,
			"target": "Рынок: после «Сделать технический MVP» или MVP≥7 (не только кликабельный макет) — задача «Вывести продукт на рынок» или кнопка. Уверенный релиз: CMO+CTO, CustDev≥6, Problem≥6, MVP≥7."
		}
	if week <= 12:
		return {
			"key": "fit",
			"name": "Fit (Недели 7-12)",
			"goal": "Найти PMF: улучшать удержание и активацию без потери команды.",
			"target": "Retention>=34, Activation>=40, Morale>=40"
		}
	return {
		"key": "scale",
		"name": "Scale (Недели 13-18)",
		"goal": "Масштабировать рост и выручку, не разрушая экономику.",
		"target": "Рост MRR, Runway>=4, Reputation>=55"
	}

func can_launch_now() -> bool:
	return product_allows_market_launch()

func _apply_team_perks() -> String:
	var lines: Array[String] = []
	if has_hr:
		morale = clamp(morale + 1.0, 0.0, 100.0)
		burn_rate = max(6000.0, burn_rate - 150.0)
		lines.append("HR (найм и культура): +1 мораль, -150 burn")
	if has_marketer:
		# Growth-маркетолог стабильно добавляет распределение и узнаваемость.
		activation = clamp(activation + 1.0, 0.0, 100.0)
		reputation = clamp(reputation + 1.0, 0.0, 100.0)
		lines.append("CMO (маркетинг и рост): +1 активация, +1 репутация")
	if has_cto:
		# CTO системно снижает хаос в коде; после запуска также влияет на удержание.
		tech_debt = clamp(tech_debt - 1.0, 0.0, 100.0)
		if launched_to_market:
			retention = clamp(retention + 1.0, 0.0, 100.0)
			lines.append("CTO (продукт и разработка): -1 техдолг, +1 удержание")
		else:
			mvp_progress = clamp(mvp_progress + 1.0, 0.0, 20.0)
			lines.append("CTO (продукт и разработка): -1 техдолг, +1 прогресс MVP")
	if has_cfo:
		# CFO уменьшает burn каждую неделю и помогает монетизации после выхода в рынок.
		burn_rate = max(6000.0, burn_rate - 400.0)
		if launched_to_market:
			revenue += 300.0
			lines.append("CFO (финансы и burn): -400 burn, +300 MRR")
		else:
			cash += 500.0
			lines.append("CFO (финансы и burn): -400 burn, +500 ₽ cash")
	if has_coo:
		morale = clamp(morale + 1.0, 0.0, 100.0)
		if launched_to_market:
			retention = clamp(retention + 1.0, 0.0, 100.0)
			lines.append("COO (операции и процессы): +1 мораль, +1 удержание")
		else:
			mvp_progress = clamp(mvp_progress + 1.0, 0.0, 20.0)
			lines.append("COO (операции и процессы): +1 мораль, +1 прогресс MVP")
	if has_cco:
		if launched_to_market:
			revenue += 450.0
			activation = clamp(activation + 1.0, 0.0, 100.0)
			lines.append("CCO (продажи и выручка): +450 MRR, +1 активация")
		else:
			custdev_points = clamp(custdev_points + 1.0, 0.0, 20.0)
			problem_clarity = clamp(problem_clarity + 1.0, 0.0, 20.0)
			lines.append("CCO (продажи и выручка): +1 CustDev, +1 ясность проблемы (дискавери)")
	if has_cpo:
		if launched_to_market:
			retention = clamp(retention + 1.0, 0.0, 100.0)
			activation = clamp(activation + 1.0, 0.0, 100.0)
			lines.append("CPO (продукт и метрики): +1 удержание, +1 активация")
		else:
			problem_clarity = clamp(problem_clarity + 1.0, 0.0, 20.0)
			mvp_progress = clamp(mvp_progress + 1.0, 0.0, 20.0)
			lines.append("CPO (продукт и метрики): +1 ясность проблемы, +1 прогресс MVP")
	if friend_count > 0:
		var f: float = float(friend_count)
		reputation = clamp(reputation + f * 0.5, 0.0, 100.0)
		morale = clamp(morale + f * 0.5, 0.0, 100.0)
		lines.append("Друзья x%d: +%.1f репутация, +%.1f мораль" % [friend_count, f * 0.5, f * 0.5])
	if lines.is_empty():
		return ""
	return "Пассивные перки команды:\n- " + "\n- ".join(lines)

func get_team_perks_summary() -> String:
	var lines: Array[String] = []
	if has_hr:
		lines.append("HR (найм и культура) -> +1 мораль/нед, -150 burn/нед")
	if has_marketer:
		lines.append("CMO (маркетинг и рост) -> +1 активация/нед, +1 репутация/нед")
	if has_cto:
		if launched_to_market:
			lines.append("CTO (продукт и разработка) -> -1 техдолг/нед, +1 удержание/нед")
		else:
			lines.append("CTO (продукт и разработка) -> -1 техдолг/нед, +1 прогресс MVP/нед")
	if has_cfo:
		if launched_to_market:
			lines.append("CFO (финансы и burn) -> -400 burn/нед, +300 MRR/нед")
		else:
			lines.append("CFO (финансы и burn) -> -400 burn/нед, +500 ₽ cash/нед")
	if has_coo:
		if launched_to_market:
			lines.append("COO (операции и процессы) -> +1 мораль/нед, +1 удержание/нед")
		else:
			lines.append("COO (операции и процессы) -> +1 мораль/нед, +1 прогресс MVP/нед")
	if has_cco:
		if launched_to_market:
			lines.append("CCO (продажи и выручка) -> +450 MRR/нед, +1 активация/нед")
		else:
			lines.append("CCO (продажи и выручка) -> +1 CustDev/нед, +1 Problem/нед (до релиза)")
	if has_cpo:
		if launched_to_market:
			lines.append("CPO (продукт и метрики) -> +1 удержание/нед, +1 активация/нед")
		else:
			lines.append("CPO (продукт и метрики) -> +1 Problem/нед, +1 MVP/нед (до релиза)")
	if friend_count > 0:
		lines.append("Друзья x%d -> +%.1f репутация/нед, +%.1f мораль/нед" % [friend_count, float(friend_count) * 0.5, float(friend_count) * 0.5])
	if lines.is_empty():
		return "Перков нет: команда не собрана."
	return "\n".join(lines)

func _default_tasks() -> Array[Dictionary]:
	return [
		# BUILD STAGE: старт без команды и без продукта
		{
			"name": "Найм сотрудников на Startup Fellows",
			"stage": "build",
			"effects": {"startup_fellows_onboard": 1, "cash": -2600, "morale": 1}
		},
		{
			"name": "Нанять HR в команду (найм и культура)",
			"stage": "build",
			"effects": {"hire_hr": 1, "burn_rate": 1600, "cash": -1600, "morale": 2}
		},
		{
			"name": "Нанять CMO в команду (маркетинг и рост)",
			"stage": "build",
			"effects": {"hire_marketer": 1, "burn_rate": 2200, "cash": -2500, "morale": 1}
		},
		{
			"name": "Нанять CTO в команду (продукт и разработка)",
			"stage": "build",
			"effects": {"hire_cto": 1, "burn_rate": 3200, "cash": -3200, "tech_debt": -2}
		},
		{
			"name": "Нанять CFO в команду (финансы и burn)",
			"stage": "build",
			"effects": {"hire_cfo": 1, "burn_rate": 1800, "cash": -1800, "morale": 1}
		},
		{
			"name": "Нанять COO в команду (операции и процессы)",
			"stage": "build",
			"effects": {"hire_coo": 1, "burn_rate": 2600, "cash": -2600, "morale": 1}
		},
		{
			"name": "Нанять CCO в команду (продажи и выручка)",
			"stage": "all",
			"effects": {"hire_cco": 1, "burn_rate": CCO_BURN, "cash": -2900, "morale": 1}
		},
		{
			"name": "Нанять CPO в команду (продукт и метрики)",
			"stage": "all",
			"effects": {"hire_cpo": 1, "burn_rate": CPO_BURN, "cash": -2700, "morale": 1}
		},
		{
			"name": "Аренда офиса в бизнес-центре класса B",
			"stage": "build",
			"effects": {"rent_office_b": 1, "cash": -5000, "morale": 3}
		},
		{
			"name": "Аренда красивого офиса в бизнес-центре класса A (очень дорого)",
			"stage": "build",
			"effects": {"rent_office_a": 1, "cash": -14000, "morale": 6, "reputation": 1}
		},
		{
			"name": "Нанять Друга в команду (дорого + доля)",
			"stage": "all",
			"effects": {"hire_friend": 1, "burn_rate": 2800, "cash": -7000, "founder_share": -4, "morale": 2}
		},
		{
			"name": "Уволить CMO (маркетинг и рост) — снизить burn",
			"stage": "all",
			"effects": {"fire_marketer": 1, "burn_rate": -2200, "morale": -4, "reputation": -1}
		},
		{
			"name": "Уволить CTO (продукт и разработка) — снизить burn",
			"stage": "all",
			"effects": {"fire_cto": 1, "burn_rate": -3200, "morale": -5, "tech_debt": 2}
		},
		{
			"name": "Уволить CFO (финансы и burn) — снизить burn",
			"stage": "all",
			"effects": {"fire_cfo": 1, "burn_rate": -1800, "morale": -3}
		},
		{
			"name": "Уволить HR (найм и культура) — снизить burn",
			"stage": "all",
			"effects": {"fire_hr": 1, "burn_rate": -1600, "morale": -3}
		},
		{
			"name": "Уволить COO (операции и процессы) — снизить burn",
			"stage": "all",
			"effects": {"fire_coo": 1, "burn_rate": -2600, "morale": -4, "retention": -1}
		},
		{
			"name": "Уволить CCO (продажи и выручка) — снизить burn",
			"stage": "all",
			"effects": {"fire_cco": 1, "burn_rate": -CCO_BURN, "morale": -4, "reputation": -1}
		},
		{
			"name": "Уволить CPO (продукт и метрики) — снизить burn",
			"stage": "all",
			"effects": {"fire_cpo": 1, "burn_rate": -CPO_BURN, "morale": -4, "reputation": -1}
		},
		{
			"name": "Уволить Друга (доля не возвращается)",
			"stage": "all",
			"effects": {"fire_friend": 1, "burn_rate": -2800, "morale": -5}
		},
		{
			"name": "Срочно: бридж-раунд (деньги vs доля)",
			"stage": "all",
			"crisis_only": true,
			"effects": {"cash": 48000, "founder_share": -9, "reputation": 2, "morale": -2}
		},
		{
			"name": "Начать вести телеграм-канал о стартапе",
			"stage": "all",
			"effects": {"reputation": 4, "morale": 1, "cash": -400, "startup_telegram_channel": 1}
		},
		{
			"name": "Desk research: черновик ICP и сегменты",
			"stage": "build",
			"effects": {"custdev_points": 2, "problem_clarity": 1, "cash": -500}
		},
		{
			"name": "Провести опрос в Яндекс.Формах о потребностях в продукте",
			"stage": "build",
			"hide_after_first_mvp": true,
			"effects": {"custdev_points": -1}
		},
		{
			"name": "Провести трендвотчинг (рынок и смежные ниши)",
			"stage": "build",
			"effects": {"custdev_points": 1, "problem_clarity": 1, "reputation": 1, "cash": -450, "morale": -1}
		},
		{
			"name": "Провести интервью с отраслевыми экспертами (не пользователи)",
			"stage": "build",
			"effects": {"custdev_points": 1, "problem_clarity": 1, "cash": -650, "morale": 1}
		},
		{
			"name": "Посетить отраслевое мероприятие / нетворк с экспертами",
			"stage": "build",
			"effects": {"custdev_points": 1, "reputation": 2, "cash": -900, "morale": 1}
		},
		{
			"name": "Воркшоп: сформулировать и проверить 3 ключевые гипотезы",
			"stage": "build",
			"requires": {"custdev_points": 2},
			"effects": {"problem_clarity": 2, "custdev_points": 1, "morale": 2, "cash": -400}
		},
		{
			"name": "10 холодных касаний: кастдев-письма сегменту",
			"stage": "build",
			"requires": {"custdev_points": 2},
			"effects": {"custdev_points": 2, "reputation": 1, "cash": -350, "morale": -1}
		},
		{
			"name": TASK_PRODUCT_ANALYTICS,
			"stage": "build",
			"requires": {"playable_prototype": 1},
			"effects": {"mvp_progress": 1, "tech_debt": 1, "activation": 1, "cash": -700}
		},
		{
			"name": "Юридический мини-аудит политики данных и оферты",
			"stage": "build",
			"requires": {"product_landing": 1},
			"effects": {"reputation": 2, "cash": -2200, "morale": -1}
		},
		{
			"name": "Ретро спринта: приоритизация backlog (RICE)",
			"stage": "build",
			"effects": {"morale": 3, "tech_debt": -1, "problem_clarity": 1}
		},
		{
			"name": "One-pager партнёра / питча для первых встреч",
			"stage": "build",
			"requires": {"problem_clarity": 2},
			"effects": {"reputation": 2, "problem_clarity": 1, "cash": -600}
		},
		{
			"name": TASK_PRODUCT_LANDING,
			"stage": "build",
			"requires": {"custdev_points": 2, "problem_clarity": 2},
			"effects": {"activation": 2, "reputation": 2, "cash": -850, "morale": -1}
		},
		{
			"name": TASK_PRODUCT_LANDING_REDO,
			"stage": "build",
			"effects": {"activation": 2, "reputation": 1, "cash": -650, "morale": -1}
		},
		{
			"name": "Встреча на митапе: нетворкинг и быстрые интервью",
			"stage": "build",
			"requires": {"custdev_points": 2},
			"insight_gain": 1.0,
			"effects": {"custdev_points": 2, "reputation": 2, "cash": -800, "morale": 1, "advisor_pipeline": 1}
		},
		{
			"name": "Найти эксперта по тематике в интернете и написать ему письмо",
			"stage": "build",
			"effects": {"custdev_points": 1, "cash": -500, "morale": -1, "advisor_pipeline": 1}
		},
		{
			"name": "Принять участие в акселераторе (Build batch)",
			"stage": "build",
			"requires": {"problem_clarity": 2},
			"effects": {"accelerator_apply": 1, "reputation": 2, "custdev_points": 1, "cash": -2000, "morale": 0}
		},
		{
			"name": "Принять участие в ГРАНТЕ (Build)",
			"stage": "build",
			"requires": {"problem_clarity": 2},
			"effects": {"grant_apply": 20000, "cash": -900, "reputation": 1}
		},
		{
			"name": "Провести 5 CustDev-интервью",
			"stage": "build",
			"task_chain": "custdev_sprint",
			"requires": {"custdev_points": 2},
			"insight_gain": 2.0,
			"effects": {"custdev_points": 3, "problem_clarity": 2, "cash": -1200}
		},
		{
			"name": "Синтезировать инсайты CustDev и описать JTBD",
			"stage": "build",
			"requires": {"custdev_points": 4},
			"effects": {"custdev_points": 2, "problem_clarity": 3, "morale": -1}
		},
		{
			"name": TASK_CLICKABLE_PROTOTYPE_ASSEMBLY,
			"stage": "build",
			"omit_if_mvp_gte": MVP_ASSEMBLY_TASK_HIDE_FROM,
			"requires": {"problem_clarity": 2},
			"insight_scaling": {"problem_clarity": 1, "activation": 1},
			"effects": {"mvp_progress": 3, "playable_prototype": 1, "tech_debt": 1, "cash": -1600}
		},
		{
			"name": TASK_TECHNICAL_MVP,
			"stage": "build",
			"omit_if_mvp_gte": MVP_ASSEMBLY_TASK_HIDE_FROM,
			"requires": {"custdev_points": 2, "problem_clarity": 2},
			"effects": {"mvp_progress": 4, "playable_prototype": 1, "technical_mvp": 1, "tech_debt": 3, "morale": -2}
		},
		{
			"name": "Проверить прототип на 5 пользователях",
			"stage": "build",
			"task_chain": "usability_wave",
			"requires": {"playable_prototype": 1, "problem_clarity": 2},
			"insight_scaling": {"retention": 2, "problem_clarity": 1},
			"effects": {"problem_clarity": 2, "retention": 1, "cash": -900}
		},
		{
			"name": "Прописать ценностное предложение и позиционирование",
			"stage": "build",
			"requires": {"custdev_points": 2},
			"effects": {"problem_clarity": 2, "reputation": 1}
		},
		{
			"name": "Вывести продукт на рынок (первый релиз)",
			"stage": "build",
			"effects": {"launch_market": 1, "reputation": 2, "cash": -1200}
		},

		# FIT STAGE
		{
			"name": TASK_FIT_PILOT_SELECT,
			"stage": "fit",
			"task_chain": "fit_pilot_wave",
			"requires": {"activation": 14, "problem_clarity": 5},
			"insight_gain": 1.5,
			"effects": {"problem_clarity": 1, "custdev_points": 1, "reputation": 2, "cash": -1800, "morale": -1}
		},
		{
			"name": TASK_FIT_PILOT_SUPPORT,
			"stage": "fit",
			"task_chain": "fit_pilot_wave",
			"requires": {"activation": 16, "retention": 14},
			"insight_scaling": {"activation": 1, "retention": 2},
			"effects": {"activation": 3, "retention": 3, "reputation": 1, "cash": -1400, "morale": -2}
		},
		{
			"name": TASK_FIT_PILOT_ITERATE,
			"stage": "fit",
			"task_chain": "fit_pilot_wave",
			"requires": {"activation": 18, "problem_clarity": 6},
			"insight_scaling": {"activation": 2, "problem_clarity": 1},
			"effects": {"activation": 2, "retention": 3, "problem_clarity": 2, "tech_debt": -1, "cash": -1600}
		},
		{
			"name": "Запустить фичу 'умные рекомендации'",
			"stage": "fit",
			"insight_scaling": {"activation": 2},
			"effects": {"activation": 5, "tech_debt": 4, "morale": -2}
		},
		{
			"name": "Починить 10 критических багов",
			"stage": "fit",
			"effects": {"retention": 4, "tech_debt": -5, "morale": -1, "reputation": 2}
		},
		{
			"name": "Провести CustDev (8 интервью)",
			"stage": "fit",
			"task_chain": "custdev_sprint",
			"insight_gain": 3.0,
			"effects": {"custdev_points": 3, "problem_clarity": 2, "activation": 2, "retention": 2, "reputation": 1, "cash": -1500}
		},
		{
			"name": "Агрессивный таргет в соцсетях",
			"stage": "fit",
			"effects": {"activation": 4, "burn_rate": 1500, "cash": -3000}
		},
		{
			"name": "Рефакторинг модуля оплаты",
			"stage": "fit",
			"effects": {"tech_debt": -6, "retention": 2, "morale": -2}
		},
		{
			"name": "Сделать демо для инвестора",
			"stage": "fit",
			"illustration": "res://assets/portrait_investor_1.png",
			"effects": {"cash": 12000, "morale": -3, "reputation": 1}
		},
		{
			"name": "Внедрить онбординг в 3 шага",
			"stage": "fit",
			"effects": {"activation": 3, "retention": 2}
		},
		{
			"name": "Нанять фриланс-разработчика на спринт",
			"stage": "fit",
			"effects": {"tech_debt": 2, "burn_rate": 2500, "morale": 1}
		},
		{
			"name": "Настроить welcome-цепочку писем для новых пользователей",
			"effects": {"activation": 3, "retention": 2, "cash": -1200}
		},
		{
			"name": "Сделать лэндинг с A/B заголовками",
			"effects": {"activation": 3, "reputation": 1, "cash": -900}
		},
		{
			"name": "Добавить paywall после 7-дневного trial",
			"effects": {"revenue": 2200, "retention": -2, "reputation": -1}
		},
		{
			"name": "Провести usability-тесты (5 сессий)",
			"stage": "fit",
			"task_chain": "usability_wave",
			"requires": {"playable_prototype": 1, "problem_clarity": 2},
			"insight_scaling": {"activation": 1, "retention": 2},
			"effects": {"activation": 2, "retention": 3, "morale": -1, "problem_clarity": 1, "cash": -1100}
		},
		{
			"name": "Разобрать churn и внедрить win-back сценарий",
			"effects": {"retention": 4, "reputation": 1, "cash": -1500}
		},
		{
			"name": "Подготовить KPI-дашборд для команды",
			"effects": {"morale": 2, "activation": 1, "retention": 1}
		},
		{
			"name": "Запустить партнерскую программу с блогерами",
			"stage": "fit",
			"effects": {"influencer_channel": 1, "activation": 4, "burn_rate": 1200, "reputation": 1}
		},
		{
			"name": "Оптимизировать скорость загрузки приложения",
			"effects": {"retention": 3, "activation": 2, "tech_debt": -2}
		},
		{
			"name": "Добавить in-app подсказки в ключевом сценарии",
			"stage": "fit",
			"insight_scaling": {"activation": 1, "retention": 1},
			"effects": {"activation": 3, "retention": 1}
		},
		{
			"name": "Подготовить enterprise-пакет для B2B",
			"effects": {"revenue": 1800, "cash": 3500, "tech_debt": 2}
		},
		{
			"name": "Автоматизировать регрессионные тесты",
			"effects": {"tech_debt": -4, "retention": 2, "morale": -1}
		},
		{
			"name": "Провести спринт рефакторинга API",
			"effects": {"tech_debt": -5, "activation": -1, "retention": 2}
		},
		{
			"name": "Запустить реферальную механику 'приведи друга'",
			"effects": {"activation": 5, "reputation": 1, "cash": -2000}
		},
		{
			"name": "Сделать контент-маркетинг на 4 недели",
			"effects": {"activation": 2, "reputation": 3, "burn_rate": 700}
		},
		{
			"name": "Обновить pricing-страницу и тарифы",
			"effects": {"revenue": 2600, "retention": -1, "reputation": -1}
		},
		{
			"name": "Внедрить чат-поддержку в рабочее время",
			"effects": {"retention": 3, "reputation": 2, "burn_rate": 900}
		},
		{
			"name": "Провести офлайн-интервью с B2B клиентами",
			"effects": {"revenue": 1400, "retention": 2, "cash": -1600}
		},
		{
			"name": "Сократить инфраструктурные расходы",
			"effects": {"burn_rate": -1800, "activation": -1, "retention": -1}
		},
		{
			"name": "Запустить beta-программу с power users",
			"effects": {"activation": 2, "retention": 3, "reputation": 2}
		},
		{
			"name": "Сделать релиз без QA, чтобы успеть к демо",
			"effects": {"activation": 3, "tech_debt": 6, "reputation": -3}
		},
		{
			"name": "Взять junior-стажера на контент и саппорт",
			"effects": {"burn_rate": 600, "morale": 2, "reputation": 1}
		},
		{
			"name": "Запустить in-app опрос NPS",
			"effects": {"reputation": 2, "retention": 1, "cash": -700}
		},
		{
			"name": "Подписать интеграцию с платежным провайдером",
			"effects": {"revenue": 1700, "tech_debt": 1, "cash": -1300}
		},
		{
			"name": "Сделать кампанию реактивации 'вернемся?'",
			"effects": {"retention": 3, "activation": 1, "cash": -1200}
		},
		{
			"name": "Провести внутренний день без митингов",
			"stage": "fit",
			"effects": {"morale": 4, "tech_debt": -1, "activation": 1}
		},
		{
			"name": "Нанять digital-агентство на GTM-спринт (реклама, креатив, отчётность)",
			"stage": "fit",
			"requires": {"problem_clarity": 3},
			"effects": {"growth_agency_contract": 1, "cash": -2200, "burn_rate": 500, "activation": 2, "reputation": 1, "morale": -1}
		},
		{
			"name": "Доработать core-сценарий: сократить путь до ценности",
			"stage": "fit",
			"requires": {"problem_clarity": 4, "mvp_progress": 3},
			"effects": {"activation": 4, "retention": 2, "tech_debt": 1, "cash": -1400}
		},
		{
			"name": "Переделать UX критического экрана оплаты",
			"stage": "fit",
			"requires": {"mvp_progress": 3},
			"effects": {"activation": 2, "retention": 3, "revenue": 1200, "cash": -1600}
		},
		{
			"name": "Ускорить продукт: оптимизация загрузки и отклика",
			"stage": "fit",
			"effects": {"activation": 2, "retention": 3, "tech_debt": -2, "cash": -1100}
		},
		{
			"name": "Доработать onboarding на основе воронки отвалов",
			"stage": "fit",
			"requires": {"activation": 10},
			"effects": {"activation": 3, "retention": 2, "problem_clarity": 1, "cash": -900}
		},
		{
			"name": "Сделать персонализацию первого пользовательского опыта",
			"stage": "fit",
			"requires": {"problem_clarity": 5},
			"effects": {"activation": 3, "retention": 2, "tech_debt": 1, "cash": -1300}
		},
		{
			"name": "Доработать стабильность релиза: фиксы после обратной связи",
			"stage": "fit",
			"requires": {"mvp_progress": 4},
			"effects": {"retention": 4, "tech_debt": -3, "reputation": 1, "cash": -1000}
		},
		{
			"name": "Переделать структуру тарифов внутри продукта",
			"stage": "fit",
			"requires": {"retention": 10},
			"effects": {"revenue": 1800, "activation": 1, "retention": -1, "cash": -800}
		},
		{
			"name": TASK_GRANT_KPI_PACK,
			"stage": "fit",
			"requires": {"problem_clarity": 5},
			"effects": {"problem_clarity": 1, "reputation": 2, "cash": -1200, "morale": -1}
		},
		{
			"name": TASK_GRANT_STATUS_SYNC,
			"stage": "fit",
			"effects": {"reputation": 2, "morale": 1, "cash": -600}
		},
		{
			"name": TASK_LEGAL_FIT,
			"stage": "fit",
			"requires": {"problem_clarity": 4},
			"effects": {"reputation": 2, "retention": 1, "cash": -1700}
		},
		{
			"name": TASK_RICE_FIT,
			"stage": "fit",
			"effects": {"morale": 2, "problem_clarity": 1, "tech_debt": -1}
		},
		{
			"name": TASK_ADVISOR_FIT,
			"stage": "fit",
			"insight_gain": 1.5,
			"effects": {"custdev_points": 1, "problem_clarity": 2, "morale": 1, "cash": -900}
		},
		{
			"name": TASK_PARTNER_FIT,
			"stage": "fit",
			"requires": {"problem_clarity": 5},
			"effects": {"revenue": 700, "reputation": 2, "cash": -1400}
		},
		{
			"name": TASK_OFFICE_FIT,
			"stage": "fit",
			"effects": {"morale": 3, "tech_debt": -1, "cash": -700}
		},
		# SCALE STAGE
		{
			"name": "Масштабировать performance-маркетинг",
			"stage": "scale",
			"requires": {"retention": 18},
			"effects": {"activation": 4, "burn_rate": 2200, "revenue": 1500}
		},
		{
			"name": "Нанять growth-агентство: performance, медиапланирование и аналитика",
			"stage": "scale",
			"requires": {"retention": 16},
			"effects": {"growth_agency_contract": 1, "cash": -3800, "burn_rate": 700, "activation": 2, "revenue": 600, "reputation": 1}
		},
		{
			"name": "Открыть B2B канал продаж",
			"stage": "scale",
			"effects": {"revenue": 3000, "cash": 2500, "tech_debt": 1}
		},
		{
			"name": "Запустить PR-кампанию на рынок",
			"stage": "scale",
			"effects": {"reputation": 4, "activation": 2, "cash": -2200}
		},
		{
			"name": "Стабилизировать масштаб (инфра + SRE)",
			"stage": "scale",
			"effects": {"tech_debt": -4, "retention": 3, "burn_rate": 900}
		},
		{
			"name": "CustDev волна 3: 12 глубинных интервью (ICP + enterprise)",
			"stage": "scale",
			"task_chain": "custdev_sprint",
			"insight_gain": 3.0,
			"effects": {"custdev_points": 4, "problem_clarity": 2, "reputation": 2, "revenue": 800, "cash": -2400, "morale": -1}
		},
		{
			"name": "UX-лаборатория: 8 сессий на реальных пользователях",
			"stage": "scale",
			"task_chain": "usability_wave",
			"requires": {"mvp_progress": 3, "problem_clarity": 3},
			"insight_gain": 2.0,
			"effects": {"activation": 4, "retention": 5, "reputation": 2, "cash": -1900, "tech_debt": 1}
		},
		{
			"name": "Сетка performance: ремаркетинг + lookalike + частотные капы",
			"stage": "scale",
			"requires": {"retention": 18},
			"effects": {"activation": 3, "revenue": 1300, "burn_rate": 1900, "reputation": 1, "cash": -900}
		},
		{
			"name": "Контекстная реклама по high-intent запросам (Search)",
			"stage": "scale",
			"effects": {"activation": 2, "revenue": 1200, "burn_rate": 1500, "cash": -700}
		},
		{
			"name": "Медийный охват: баннеры и натив под узнаваемость (CPM)",
			"stage": "scale",
			"effects": {"reputation": 3, "activation": 3, "revenue": 500, "burn_rate": 2600, "morale": -1, "cash": -1200}
		},
		{
			"name": "Аффилиаты и партнёры: CPA/RevShare и промо-коды",
			"stage": "scale",
			"effects": {"revenue": 1000, "activation": 2, "burn_rate": 700, "cash": -1100, "reputation": 1}
		},
		{
			"name": "Маркетплейсы и каталоги: листинг + отзывы (G2/Capterra и аналоги)",
			"stage": "scale",
			"effects": {"activation": 3, "reputation": 2, "revenue": 800, "cash": -1600, "morale": -1}
		},
		{
			"name": "Lifecycle-email: триггеры, дайджест, double opt-in и compliance",
			"stage": "scale",
			"requires": {"activation": 18},
			"effects": {"retention": 3, "activation": 2, "revenue": 600, "reputation": 1, "cash": -650, "tech_debt": 1}
		},
		{
			"name": "Холодные email-цепочки (S sequences) по ICP в B2B",
			"stage": "scale",
			"requires": {"problem_clarity": 4},
			"effects": {"revenue": 1500, "activation": 1, "reputation": -1, "morale": -2, "cash": -450}
		},
		{
			"name": "Outbound в LinkedIn: Sales Nav, тёплые интро и follow-up",
			"stage": "scale",
			"requires": {"problem_clarity": 4},
			"effects": {"revenue": 1700, "burn_rate": 850, "morale": -1, "cash": -750, "reputation": 1}
		},
		{
			"name": "Спонсорство подкастов и отраслевых рассылок (нишевые медиа)",
			"stage": "scale",
			"effects": {"influencer_channel": 1, "reputation": 3, "activation": 2, "revenue": 900, "cash": -1900}
		},
		{
			"name": "SEO: контент-кластеры, обновления и long-tail под спрос",
			"stage": "scale",
			"effects": {"activation": 2, "revenue": 1200, "burn_rate": 550, "cash": -850, "tech_debt": 1}
		},
		{
			"name": "Вебинары как воронка: лиды, запись, nurture после эфира",
			"stage": "scale",
			"effects": {"activation": 4, "reputation": 2, "revenue": 700, "cash": -1500, "morale": -1}
		},
		{
			"name": "Push и in-app кампании: реактивация, апсейл, сегменты",
			"stage": "scale",
			"requires": {"retention": 16},
			"effects": {"retention": 2, "revenue": 950, "activation": 1, "tech_debt": 1, "cash": -500}
		},
		{
			"name": "Реферальная программа для SMB: двусторонний бонус и трекинг",
			"stage": "scale",
			"effects": {"activation": 3, "revenue": 1400, "morale": 1, "cash": -900, "retention": 1}
		},
		{
			"name": "UGC в коротком видео: креаторы, брифы, тест креативов",
			"stage": "scale",
			"effects": {"influencer_channel": 1, "activation": 5, "reputation": 2, "revenue": 450, "burn_rate": 2100, "morale": -1, "cash": -1300}
		},
		{
			"name": "Канал интеграций: co-marketing с платформами и партнёрские вебинары",
			"stage": "scale",
			"requires": {"mvp_progress": 4},
			"effects": {"revenue": 1800, "reputation": 2, "tech_debt": 2, "cash": -1200, "activation": 1}
		},
		{
			"name": "Принять участие в акселераторе (Scale/международный трек)",
			"stage": "scale",
			"requires": {"retention": 20},
			"effects": {"accelerator_apply": 1, "reputation": 3, "revenue": 900, "cash": -2800, "morale": 0}
		},
		{
			"name": TASK_GRANT_FINAL_AUDIT,
			"stage": "scale",
			"effects": {"reputation": 3, "morale": 1, "cash": -1500}
		},
		{
			"name": TASK_COMPLIANCE_SCALE,
			"stage": "scale",
			"requires": {"reputation": 35, "retention": 20},
			"effects": {"reputation": 3, "revenue": 1200, "cash": -2200}
		},
		{
			"name": TASK_RICE_SCALE,
			"stage": "scale",
			"effects": {"morale": 2, "retention": 1, "tech_debt": -1}
		},
		{
			"name": TASK_ADVISOR_SCALE,
			"stage": "scale",
			"insight_gain": 2.0,
			"effects": {"problem_clarity": 1, "revenue": 900, "reputation": 2, "cash": -1600}
		},
		{
			"name": TASK_PARTNER_SCALE,
			"stage": "scale",
			"requires": {"reputation": 35},
			"effects": {"revenue": 1400, "reputation": 2, "cash": -1800}
		},
		{
			"name": TASK_OFFICE_SCALE,
			"stage": "scale",
			"effects": {"morale": 3, "retention": 1, "burn_rate": 500, "cash": -900}
		},
		{
			"name": "Принять участие в ГРАНТЕ (Scale)",
			"stage": "scale",
			"requires": {"activation": 20},
			"effects": {"grant_apply": 34000, "cash": -1800, "reputation": 1}
		}
	]

func _default_events() -> Array[Dictionary]:
	return [
		{
			"type": "Инвестор",
			"requires_investor_contact": true,
			"only_before_launch": true,
			"investor_portrait": 1,
			"illustration": "res://assets/portrait_investor_1.png",
			"title": "Инвестор спрашивает: где x3 рост?",
			"options": [
				{
					"text": "Обещать и ускорить релиз любой ценой",
					"effects": {"activation": 3, "tech_debt": 5, "morale": -4}
				},
				{
					"text": "Показать честные метрики и план",
					"effects": {"reputation": 3, "cash": -2000}
				}
			]
		},
		{
			"type": "Техника",
			"requires_live_product": true,
			"title": "Прод упал вечером в пятницу.",
			"options": [
				{
					"text": "Экстренно чиним ночью",
					"effects": {"retention": 2, "morale": -5, "cash": -1500}
				},
				{
					"text": "Откладываем до понедельника",
					"effects": {"morale": 2, "reputation": -5, "retention": -3}
				}
			]
		},
		{
			"type": "Техника",
			"stage": "build",
			"only_before_launch": true,
			"requires_playable_prototype": true,
			"title": "Локальное демо зависло перед созвоном с пользователями.",
			"options": [
				{
					"text": "Чиним на коленке прямо в переговорке",
					"effects": {"morale": -2, "problem_clarity": 1, "cash": -400}
				},
				{
					"text": "Честно переносим: «техническая пауза 10 минут»",
					"effects": {"reputation": 1, "morale": -1, "custdev_points": 1}
				}
			]
		},
		{
			"type": "Техника",
			"stage": "build",
			"only_before_launch": true,
			"requires_playable_prototype": true,
			"title": "В прототипе всплыл баг: неверно сохраняются ответы формы.",
			"options": [
				{
					"text": "Патчим срочно, даже если задержим интервью",
					"effects": {"mvp_progress": 1, "tech_debt": 2, "morale": -2}
				},
				{
					"text": "Фиксируем как известный риск и ведём CustDev дальше",
					"effects": {"custdev_points": 2, "tech_debt": 1, "problem_clarity": 1}
				}
			]
		},
		{
			"type": "Продукт",
			"stage": "build",
			"only_before_launch": true,
			"title": "Команда спорит: добиваем MVP или ещё неделю только CustDev?",
			"options": [
				{
					"text": "Жёсткий фокус: закрываем сборку для релиза",
					"effects": {"mvp_progress": 2, "morale": -3, "tech_debt": 1}
				},
				{
					"text": "Ещё интервью: рискуем сдвинуть дату",
					"effects": {"custdev_points": 2, "problem_clarity": 2, "morale": 1}
				}
			]
		},
		{
			"type": "Адвайзер",
			"stage": "build",
			"only_before_launch": true,
			"title": "Адвайзер давит: «вы в неправильной нише, режьте сегмент».",
			"options": [
				{
					"text": "Режем ICP и переписываем гипотезу",
					"effects": {"problem_clarity": 2, "morale": -2, "reputation": 1}
				},
				{
					"text": "Собираем доказательства ещё неделю, потом решим",
					"effects": {"custdev_points": 2, "cash": -800, "morale": 1}
				}
			]
		},
		{
			"type": "Продукт",
			"stage": "build",
			"only_before_launch": true,
			"max_mvp_progress": 1.5,
			"requires_playable_prototype": true,
			"title": "Пилотный пользователь «потерялся»: демо сорвалось.",
			"options": [
				{
					"text": "Догнать в личке и переназначить созвон",
					"effects": {"custdev_points": 1, "morale": -2, "cash": -300}
				},
				{
					"text": "Идём к следующему лиду без паники",
					"effects": {"reputation": -1, "morale": 1, "problem_clarity": 1}
				}
			]
		},
		{
			"type": "Продукт",
			"stage": "build",
			"only_before_launch": true,
			"max_mvp_progress": 1.0,
			"title": "Файл с описанием проекта был случайно удален с компьютера.",
			"options": [
				{
					"text": "Восстановить структуру из заметок и памяти",
					"effects": {"problem_clarity": -1, "morale": -2, "cash": -200}
				},
				{
					"text": "Собрать новый one-pager на базе интервью с экспертами",
					"effects": {"custdev_points": 1, "problem_clarity": 1, "cash": -350, "morale": -1}
				}
			]
		},
		{
			"type": "PR/Репутация",
			"stage": "build",
			"only_before_launch": true,
			"max_mvp_progress": 1.0,
			"title": "Описание проекта случайно было отправлено конкуренту.",
			"options": [
				{
					"text": "Срочно переписать позиционирование и ключевые гипотезы",
					"effects": {"problem_clarity": 1, "cash": -500, "morale": -1}
				},
				{
					"text": "Игнорировать и продолжать текущий план",
					"effects": {"reputation": -2, "morale": -2}
				}
			]
		},
		{
			"type": "Продукт",
			"stage": "build",
			"only_before_launch": true,
			"requires_custdev_activity": true,
			"title": "Респондент во время CustDev очень жестко прошелся по идее продукта.",
			"options": [
				{
					"text": "Поблагодарить за жесткий фидбек и переписать гипотезу",
					"effects": {"problem_clarity": 2, "morale": -2, "reputation": 1}
				},
				{
					"text": "Списать на токсичность и оставить курс без изменений",
					"effects": {"problem_clarity": -1, "morale": -1, "reputation": -2}
				}
			]
		},
		{
			"type": "PR/Репутация",
			"stage": "build",
			"only_before_launch": true,
			"requires_custdev_activity": true,
			"title": "Респонденту во время CustDev стало плохо: пришлось вызывать скорую. Плохой инфоповод.",
			"options": [
				{
					"text": "Публично извиниться и ввести этичный регламент интервью",
					"effects": {"reputation": 1, "cash": -900, "problem_clarity": 1}
				},
				{
					"text": "Скрыть кейс и «не поднимать шум»",
					"effects": {"reputation": -4, "morale": -2}
				}
			]
		},
		{
			"type": "Адвайзер",
			"stage": "build",
			"only_before_launch": true,
			"requires_expert_search_activity": true,
			"title": "Эксперт отменил созвон за 10 минут: окно обратной связи сорвано.",
			"options": [
				{
					"text": "Оперативно искать замену через сеть контактов",
					"effects": {"cash": -300, "custdev_points": 1, "morale": -1}
				},
				{
					"text": "Перенести и использовать время на структурирование вопросов",
					"effects": {"problem_clarity": 1, "morale": 1}
				}
			]
		},
		{
			"type": "Продукт",
			"stage": "build",
			"only_before_launch": true,
			"requires_interview_activity": true,
			"title": "Потерялись заметки по интервью: часть инсайтов пропала.",
			"options": [
				{
					"text": "Восстановить по памяти и созвонам с командой",
					"effects": {"problem_clarity": -1, "morale": -2, "cash": -250}
				},
				{
					"text": "Повторить 3 интервью по самым рискованным гипотезам",
					"effects": {"custdev_points": 2, "cash": -700, "morale": -1}
				}
			]
		},
		{
			"type": "Команда",
			"stage": "build",
			"only_before_launch": true,
			"requires_friend_equity_partner": true,
			"title": "Конфликт гипотез у софаундера: друг требует сменить фокус продукта.",
			"options": [
				{
					"text": "Идти в компромисс: тестируем обе гипотезы параллельно",
					"effects": {"cash": -1000, "morale": 1, "problem_clarity": 1}
				},
				{
					"text": "Оставить только текущий курс и жестко зафиксировать приоритеты",
					"effects": {"morale": -3, "problem_clarity": 2, "reputation": -1}
				}
			]
		},
		{
			"type": "Команда",
			"stage": "build",
			"only_before_launch": true,
			"requires_roles": ["CFO"],
			"title": "Тонкий runway: CFO предлагает заморозить всё, кроме релиза.",
			"options": [
				{
					"text": "Замораживаем nice-to-have",
					"effects": {"burn_rate": -1500, "morale": -3, "mvp_progress": 1}
				},
				{
					"text": "Держим темп, ищем бридж отдельно",
					"effects": {"morale": 2, "tech_debt": 1, "cash": -500}
				}
			]
		},
		{
			"type": "Рынок",
			"title": "Конкурент запустил похожую фичу.",
			"options": [
				{
					"text": "Быстро копируем, чтобы не отстать",
					"effects": {"activation": 2, "tech_debt": 4}
				},
				{
					"text": "Упираем на качество и поддержку",
					"effects": {"retention": 3, "reputation": 2}
				}
			]
		},
		{
			"type": "Команда",
			"title": "Сеньор разработчик просит +30% к зарплате.",
			"options": [
				{
					"text": "Согласиться: важен темп команды",
					"effects": {"burn_rate": 3000, "morale": 4, "tech_debt": -1}
				},
				{
					"text": "Отказать: держим расходы под контролем",
					"effects": {"morale": -5, "cash": 1000, "tech_debt": 2}
				}
			]
		},
		{
			"type": "Продукт",
			"min_mvp_progress": 4.0,
			"title": "Пользователи просят простую, но массовую фичу.",
			"options": [
				{
					"text": "Делаем быстро и массово",
					"effects": {"activation": 4, "retention": 1, "tech_debt": 2}
				},
				{
					"text": "Откладываем, фокус на core-job",
					"effects": {"activation": -1, "retention": 3, "reputation": 1}
				}
			]
		},
		{
			"type": "PR/Репутация",
			"requires_live_product": true,
			"title": "Блогер написал резкий пост о вашем UX.",
			"options": [
				{
					"text": "Публично ответить и исправить проблемы",
					"effects": {"reputation": 3, "cash": -1200, "retention": 1}
				},
				{
					"text": "Игнорировать и не раздувать скандал",
					"effects": {"reputation": -4, "morale": 1}
				}
			]
		},
		{
			"type": "PR/Репутация",
			"stage": "all",
			"requires_startup_telegram_channel": true,
			"title": "Спам в телеграм-канале о стартапе и плохие реакции на посты.",
			"options": [
				{
					"text": "Удалить канал и начать с чистого листа (потом можно завести снова)",
					"effects": {"startup_telegram_channel_close": 1, "morale": -2, "reputation": -1}
				},
				{
					"text": "Ничего не делать, надеяться что «сама уляжется»",
					"effects": {"morale": -5, "reputation": -5}
				}
			]
		},
		{
			"type": "Инвестор",
			"investor_contact": true,
			"investor_portrait": 2,
			"illustration": "res://assets/portrait_investor_2.png",
			"title": "Фонд предлагает деньги, но хочет жесткий контроль.",
			"options": [
				{
					"text": "Берем деньги и принимаем условия",
					"effects": {"cash": 18000, "morale": -3, "reputation": 1, "founder_share": -7}
				},
				{
					"text": "Отказываемся, сохраняем свободу",
					"effects": {"reputation": 2, "cash": -1500, "morale": 2}
				}
			]
		},
		{
			"type": "Рынок",
			"requires_live_product": true,
			"title": "Рекламные ставки выросли: CAC улетел вверх.",
			"options": [
				{
					"text": "Сохранить бюджет и бороться за трафик",
					"effects": {"cash": -2500, "activation": 2, "burn_rate": 800}
				},
				{
					"text": "Снизить закупку и сделать контент-ставку",
					"effects": {"activation": -1, "retention": 2, "burn_rate": -600}
				}
			]
		},
		{
			"type": "Команда",
			"min_mvp_progress": 3.0,
			"title": "Дизайнер предлагает неделю на полный редизайн.",
			"options": [
				{
					"text": "Согласиться на редизайн",
					"effects": {"activation": 3, "retention": 2, "morale": -2}
				},
				{
					"text": "Ограничиться точечными улучшениями",
					"effects": {"activation": 1, "retention": 1, "morale": 1}
				}
			]
		},
		{
			"type": "Техника",
			"min_mvp_progress": 2.0,
			"requires_roles": ["CTO"],
			"title": "DevOps предупреждает: нужен апгрейд инфраструктуры.",
			"options": [
				{
					"text": "Делаем апгрейд сейчас",
					"effects": {"cash": -2200, "tech_debt": -4, "retention": 2}
				},
				{
					"text": "Тянем еще месяц",
					"effects": {"cash": 500, "tech_debt": 4, "reputation": -2}
				}
			]
		},
		{
			"type": "Продукт",
			"title": "CustDev показал новый сегмент с лучшим откликом.",
			"options": [
				{
					"text": "Делаем мини-пивот под сегмент",
					"effects": {"activation": 3, "retention": 3, "morale": -1}
				},
				{
					"text": "Игнорируем: слишком рискованно менять курс",
					"effects": {"activation": -2, "retention": -1, "morale": 1}
				}
			]
		},
		{
			"type": "PR/Репутация",
			"min_mvp_progress": 2.0,
			"title": "Вас позвали в подкаст про стартапы.",
			"options": [
				{
					"text": "Идем и рассказываем честно про провалы",
					"effects": {"reputation": 4, "activation": 1, "morale": 1}
				},
				{
					"text": "Отказываемся: нет времени",
					"effects": {"reputation": -1, "morale": 1}
				}
			]
		},
		{
			"type": "Рынок",
			"requires_live_product": true,
			"title": "Крупный клиент готов подписаться, но просит кастом.",
			"options": [
				{
					"text": "Берем сделку и делаем кастом",
					"effects": {"cash": 9000, "revenue": 2500, "tech_debt": 3}
				},
				{
					"text": "Отказываемся и держим продуктовый фокус",
					"effects": {"cash": -1000, "retention": 2, "tech_debt": -1}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "fit",
			"requires_live_product": true,
			"title": "Пользователи жалуются: тарифы непонятные и «слишком хитрые».",
			"options": [
				{
					"text": "Упростить сетку до 3 прозрачных планов",
					"effects": {"revenue": 1400, "retention": 2, "reputation": 2, "cash": -900}
				},
				{
					"text": "Оставить как есть, но усилить продающие подсказки",
					"effects": {"revenue": 2100, "retention": -2, "reputation": -1}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "fit",
			"requires_live_product": true,
			"requires_roles": ["CFO"],
			"title": "Финансы предлагают поднять цены на 20% уже в этом месяце.",
			"options": [
				{
					"text": "Поднимаем цены для всех текущих и новых клиентов",
					"effects": {"revenue": 2800, "retention": -3, "reputation": -2}
				},
				{
					"text": "Поднимаем только для новых клиентов, старым оставляем grandfathering",
					"effects": {"revenue": 1700, "retention": 1, "reputation": 1}
				}
			]
		},
		{
			"type": "Команда",
			"stage": "fit",
			"requires_live_product": true,
			"title": "Сотрудник, нанятый через Startup Fellows, перестал выходить на связь.",
			"options": [
				{
					"text": "Связываемся ещё: звонки, письма, эскалация через куратора программы",
					"effects": {"morale": -2, "cash": -450, "custdev_points": 1, "reputation": 1}
				},
				{
					"text": "Ищем через Startup Fellows нового сотрудника на эту роль",
					"effects": {"cash": -3200, "morale": -1, "reputation": 1, "burn_rate": 400}
				}
			]
		},
		{
			"type": "Продукт",
			"stage": "fit",
			"requires_live_product": true,
			"title": "Монетизация предлагает сдвинуть paywall на более ранний шаг.",
			"options": [
				{
					"text": "Сдвигаем paywall ближе к первому value-moment",
					"effects": {"revenue": 2200, "activation": -2, "retention": -1}
				},
				{
					"text": "Оставляем текущий момент paywall, но улучшаем объяснение ценности",
					"effects": {"revenue": 1300, "activation": 1, "retention": 1, "cash": -700}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "scale",
			"requires_live_product": true,
			"title": "Крупный клиент готов перейти на годовую предоплату со скидкой.",
			"options": [
				{
					"text": "Соглашаемся на скидку за предоплату",
					"effects": {"cash": 15000, "revenue": 1200, "retention": 2}
				},
				{
					"text": "Держим цену без скидки",
					"effects": {"revenue": 2000, "cash": -1000, "reputation": -1}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "fit",
			"requires_live_product": true,
			"title": "Появился спрос на новый тариф «Команда до 10 мест».",
			"options": [
				{
					"text": "Запускаем новый тариф быстро",
					"effects": {"revenue": 2400, "tech_debt": 2, "morale": -1}
				},
				{
					"text": "Сначала валидируем ценность через пилот",
					"effects": {"revenue": 1200, "retention": 2, "problem_clarity": 1}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "fit",
			"requires_live_product": true,
			"title": "Блокировки и ограничения в рекламных кабинетах: часть креативов и гео отключена — поток лидов просел.",
			"options": [
				{
					"text": "Срочно перекладываем бюджет в другие каналы и креативы под правила площадки",
					"effects": {"cash": -1400, "activation": 2, "revenue": 600, "burn_rate": 400, "morale": -1}
				},
				{
					"text": "Ждём модерацию и усиливаем органику, партнёрок и продуктовый контент",
					"effects": {"activation": -2, "reputation": 1, "cash": -500, "retention": 1}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "fit",
			"requires_live_product": true,
			"title": "Площадка лидогенерации ограничила размещение в вашей категории — входящие заявки падают.",
			"options": [
				{
					"text": "Платим за приоритет в выдаче и расширяем квалификацию лидов",
					"effects": {"revenue": 1500, "cash": -1100, "activation": 1, "retention": -1}
				},
				{
					"text": "Уходим в прямые сделки и ивенты, снижаем зависимость от площадки",
					"effects": {"cash": -900, "reputation": 2, "activation": -1, "problem_clarity": 1}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "fit",
			"requires_live_product": true,
			"title": "Спор про цены: поднять базовый тариф или ввести промежуточную линейку «Pro» между планами.",
			"options": [
				{
					"text": "Поднимаем базовые цены и обновляем коммуникацию ценности",
					"effects": {"revenue": 2200, "retention": -2, "reputation": -1, "morale": -1}
				},
				{
					"text": "Вводим линейку Pro без трогания входного тарифа",
					"effects": {"revenue": 1800, "activation": 1, "cash": -700, "tech_debt": 1}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "fit",
			"requires_live_product": true,
			"title": "Конкурент поднял цены: рынок ждёт, подтянем ли наши тарифы к новой «планке».",
			"options": [
				{
					"text": "Подстраиваем ценовую сетку и акценты на упаковке",
					"effects": {"revenue": 2600, "activation": -1, "reputation": 1, "cash": -500}
				},
				{
					"text": "Держим цену ниже конкурента и бьём в сегмент чувствительный к цене",
					"effects": {"revenue": 1100, "retention": 2, "activation": 2, "burn_rate": 300}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "scale",
			"requires_live_product": true,
			"title": "Продажи просят скидки «под закрытие квартала».",
			"options": [
				{
					"text": "Даём скидки агрессивно, лишь бы закрыть план",
					"effects": {"revenue": 2600, "reputation": -2, "retention": -1}
				},
				{
					"text": "Вводим лимит скидок и единый approval крупных сделок",
					"effects": {"revenue": 1400, "reputation": 1, "cash": 500}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "scale",
			"requires_live_product": true,
			"title": "Конкурент купил брендовый трафик: ваши запросы в поиске перехватывают.",
			"options": [
				{
					"text": "Отвечаем контр-кампанией и юридическим письмом",
					"effects": {"cash": -3200, "activation": 3, "reputation": 2, "burn_rate": 900}
				},
				{
					"text": "Уходим в органику, партнёрки и продуктовый бренд",
					"effects": {"activation": -1, "retention": 2, "reputation": 1, "cash": -800}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "scale",
			"requires_live_product": true,
			"title": "Рекламный кабинет ушёл в модерацию: кампании на паузе.",
			"options": [
				{
					"text": "Перекладываем бюджет в другие каналы на неделю",
					"effects": {"cash": -1800, "activation": 1, "revenue": 800, "burn_rate": 600}
				},
				{
					"text": "Ждём апелляцию и режем закупку",
					"effects": {"activation": -2, "morale": 2, "cash": 400, "reputation": -1}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "scale",
			"requires_live_product": true,
			"title": "Крупный внешний канал лидов меняет условия: комиссия, приоритет в выдаче, SLA или доступ к API.",
			"options": [
				{
					"text": "Принимаем новые правила и платим дороже ради потока",
					"effects": {"revenue": 1800, "cash": -2400, "retention": -1}
				},
				{
					"text": "Снижаем зависимость: прямые сделки, партнёрки и свой GTM",
					"effects": {"cash": -1600, "reputation": 1, "retention": 2, "activation": 1}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "scale",
			"requires_live_product": true,
			"title": "Из-за блокировок и ограничений провайдеров часть платёжных и рекламных каналов недоступна: лиды и оплаты просели.",
			"options": [
				{
					"text": "Подключаем резервные эквайринги и альтернативные площадки закупки",
					"effects": {"cash": -4200, "revenue": 900, "activation": 2, "retention": 1, "morale": -1}
				},
				{
					"text": "Фокус на рынки без ограничений и на прямые контракты с клиентами",
					"effects": {"reputation": 2, "activation": -1, "revenue": 500, "cash": -1800}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "scale",
			"requires_live_product": true,
			"title": "Площадки и соцсети усилили модерацию: часть объявлений и ключей в стоп-листе — охват и лиды ниже.",
			"options": [
				{
					"text": "Переписываем креативы и посадочные под новые правила, тестируем гипотезы",
					"effects": {"cash": -2100, "activation": 2, "revenue": 1100, "tech_debt": 1}
				},
				{
					"text": "Смещаем бюджет в каналы без жёстких блокировок (партнёрки, ивенты, sales)",
					"effects": {"cash": -2600, "reputation": 1, "retention": 1, "burn_rate": 500}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "scale",
			"requires_live_product": true,
			"title": "Коммерция предлагает индексацию цен на 8–12% и новую enterprise-сетку: минимальный чек и годовые контракты.",
			"options": [
				{
					"text": "Вводим индексацию и отдельную линейку для enterprise со скидкой за год",
					"effects": {"revenue": 3200, "retention": -2, "reputation": 1, "cash": -900, "morale": -1}
				},
				{
					"text": "Только индексация для текущих клиентов, enterprise — отдельным пилотом",
					"effects": {"revenue": 2400, "activation": -1, "cash": -400}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "scale",
			"requires_live_product": true,
			"title": "Партнёрский канал (аффилиаты и реселлеры) режет выплаты и приоритет в выдаче — поток лидов нестабилен.",
			"options": [
				{
					"text": "Пересобираем партнёрскую программу: ставки, SLA и маркетинговая поддержка",
					"effects": {"cash": -1900, "revenue": 1700, "retention": 2, "burn_rate": 400}
				},
				{
					"text": "Сужаем число партнёров и усиливаем прямой enterprise-sales",
					"effects": {"revenue": 2100, "activation": 1, "cash": -1200, "morale": -2}
				}
			]
		},
		{
			"type": "PR/Репутация",
			"stage": "scale",
			"requires_live_product": true,
			"title": "Крупная рассылка ушла в спам-папки: открываемости почти нет.",
			"options": [
				{
					"text": "Чиним доменную репутацию, warm-up, double opt-in",
					"effects": {"cash": -1400, "retention": 3, "reputation": 2, "tech_debt": 1}
				},
				{
					"text": "Режем объём и переходим на сегменты «только активные»",
					"effects": {"activation": -1, "retention": 1, "cash": 600, "reputation": -1}
				}
			]
		},
		{
			"type": "Рынок",
			"stage": "scale",
			"requires_live_product": true,
			"requires_growth_agency": true,
			"title": "Агентство growth прислало счёт «за оптимизацию» вдвое выше сметы.",
			"options": [
				{
					"text": "Платим и требуем план исправления KPI",
					"effects": {"cash": -4000, "activation": 2, "revenue": 700, "morale": -1}
				},
				{
					"text": "Рвём контракт и забираем медиапланы in-house",
					"effects": {"growth_agency_churn": 1, "morale": -3, "reputation": 1, "burn_rate": 500, "activation": -1}
				}
			]
		},
		{
			"type": "Продукт",
			"stage": "scale",
			"requires_live_product": true,
			"title": "Вебинар собрал регистрации, но ноу-шоу 70%: воронка «мёртвая».",
			"options": [
				{
					"text": "Перепаковываем оффер, напоминания и бонус за приход",
					"effects": {"cash": -900, "activation": 3, "reputation": 1, "morale": 1}
				},
				{
					"text": "Закрываем формат вебинаров и переходим в записи + sales outreach",
					"effects": {"activation": 1, "cash": 400, "revenue": 500, "morale": -1}
				}
			]
		},
		{
			"type": "Техника",
			"stage": "scale",
			"requires_live_product": true,
			"min_mvp_progress": 3.0,
			"title": "Партнёрская платформа грозится отключить API: интеграции под угрозой.",
			"options": [
				{
					"text": "Срочный рефакторинг и миграция на новый endpoint",
					"effects": {"cash": -3500, "tech_debt": -2, "retention": 2, "morale": -2}
				},
				{
					"text": "Временный костыль + ручной импорт для ключевых клиентов",
					"effects": {"tech_debt": 3, "reputation": -1, "cash": -800, "retention": 1}
				}
			]
		},
		{
			"type": "Инвестор",
			"stage": "scale",
			"requires_live_product": true,
			"requires_investor_contact": true,
			"investor_portrait": 1,
			"illustration": "res://assets/portrait_investor_1.png",
			"title": "Инвестор требует план масштабирования: новый гео или новый канал за квартал.",
			"options": [
				{
					"text": "Обещаем второй гео и наращиваем GTM-команду",
					"effects": {"reputation": 2, "burn_rate": 1500, "cash": -2000, "activation": 2}
				},
				{
					"text": "Честно: укрепляем unit-экономику в текущем рынке",
					"effects": {"reputation": 3, "retention": 2, "revenue": 600, "morale": 1}
				}
			]
		},
		{
			"type": "PR/Репутация",
			"stage": "scale",
			"requires_live_product": true,
			"requires_influencer_channel": true,
			"title": "Инфлюенсер требует пересмотреть спонсорство: «либо x2 бюджет, либо пост с извинениями».",
			"options": [
				{
					"text": "Платим больше и фиксируем жёсткий бриф",
					"effects": {"cash": -2800, "activation": 3, "reputation": 2}
				},
				{
					"text": "Рвём сделку и переводим бюджет в микро-креаторов",
					"effects": {"reputation": -2, "activation": 1, "cash": 500, "morale": 1}
				}
			]
		},
		{
			"type": "Команда",
			"stage": "scale",
			"requires_any_hire": true,
			"requires_roles": ["CCO"],
			"title": "SDR/продажи требуют удвоить квоты: «иначе не закроем план».",
			"options": [
				{
					"text": "Даём агрессивные планы и бонусы",
					"effects": {"revenue": 2200, "morale": -3, "burn_rate": 800, "reputation": -1}
				},
				{
					"text": "Пересобираем ICP, скрипты и качество лидов",
					"effects": {"reputation": 2, "retention": 2, "cash": -1200, "revenue": 900}
				}
			]
		},
		{
			"type": "Команда",
			"stage": "scale",
			"requires_roles": ["CTO"],
			"title": "CTO пригрозил уйти и сделать такой же продукт у другого работодателя, если не дадут заметно бОльшую долю.",
			"options": [
				{
					"text": "Расширяем equity с жёстким вестингом и cliff",
					"effects": {"founder_share": -6, "morale": 4, "tech_debt": -2, "reputation": 1}
				},
				{
					"text": "Не расширяем долю: юристы, NDA и золотой парашют кэшем",
					"effects": {"cash": -8500, "burn_rate": 1000, "morale": -5, "reputation": -1, "tech_debt": 1}
				}
			]
		},
		{
			"type": "Команда",
			"stage": "scale",
			"requires_roles": ["CMO"],
			"title": "CMO пригрозил уйти и увести к конкуренту ключевых клиентов и контакты партнёров.",
			"options": [
				{
					"text": "Доп. доля + чёткий earn-out при удержании клиентов",
					"effects": {"founder_share": -5, "morale": 3, "revenue": 800, "retention": 1}
				},
				{
					"text": "Не уступаем: юрблок, план замены и жёсткий handover",
					"effects": {"cash": -4500, "retention": -3, "reputation": -2, "morale": -4, "activation": -1}
				}
			]
		},
		{
			"type": "Команда",
			"title": "В команде спор: скорость или качество?",
			"options": [
				{
					"text": "Топим за скорость релизов",
					"effects": {"activation": 2, "tech_debt": 3, "morale": -1}
				},
				{
					"text": "Топим за качество и ритм",
					"effects": {"retention": 2, "tech_debt": -2, "morale": 2}
				}
			]
		},
		{
			"type": "Команда",
			"requires_any_hire": true,
			"requires_roles": ["CMO"],
			"title": "CMO просит долю за вклад в рост: 3% сейчас.",
			"options": [
				{
					"text": "Даём долю, закрепляем лояльность",
					"effects": {"founder_share": -3, "morale": 3, "reputation": 1}
				},
				{
					"text": "Отказываемся, обещаем бонус после раунда",
					"effects": {"morale": -4, "reputation": -1}
				}
			]
		},
		{
			"type": "Команда",
			"requires_any_hire": true,
			"requires_roles": ["CTO"],
			"title": "CTO требует пересмотра: иначе уйдёт к конкуренту.",
			"options": [
				{
					"text": "Выделяем 4% под будущее участие (vesting)",
					"effects": {"founder_share": -4, "morale": 4, "tech_debt": -2}
				},
				{
					"text": "Не даём equity, поднимаем cash-бонусом",
					"effects": {"burn_rate": 1800, "cash": -2000, "morale": 1}
				}
			]
		},
		{
			"type": "Команда",
			"requires_any_hire": true,
			"title": "Команда хочет ESOP-пул 5% «за риск до зарплат».",
			"options": [
				{
					"text": "Согласовать пул и вестинг",
					"effects": {"founder_share": -5, "morale": 5, "reputation": 2}
				},
				{
					"text": "Отложить до инвест-раунда",
					"effects": {"morale": -3, "reputation": -2}
				}
			]
		},
		{
			"type": "Команда",
			"requires_friend": true,
			"title": "Друг с долей устал и ставит ультиматум.",
			"options": [
				{
					"text": "Ок, выкупаем его долю и расходимся мирно",
					"effects": {"fire_friend": 1, "cash": -18000, "founder_share": 4, "morale": -2}
				},
				{
					"text": "Уговариваем остаться: расширяем его долю",
					"effects": {"founder_share": -3, "morale": 1, "reputation": 1}
				}
			]
		},
		{
			"type": "Команда",
			"stage": "build",
			"requires_office": true,
			"title": "Команда просит купить смузи-бар в офисе.",
			"options": [
				{
					"text": "Берем смузи-машину и подписку на фрукты",
					"effects": {"morale": 3, "burn_rate": 500, "cash": -1200, "smoothie_bar": 1}
				},
				{
					"text": "Оставляем воду и чай, фокус на продукт",
					"effects": {"morale": -2, "burn_rate": -200}
				}
			]
		},
		{
			"type": "Команда",
			"stage": "build",
			"requires_office": true,
			"title": "Команда просит нанять кофе-леди в офис.",
			"options": [
				{
					"text": "Нанимаем кофе-леди",
					"effects": {"morale": 4, "burn_rate": 1200, "cash": -2000, "coffee_lady": 1}
				},
				{
					"text": "Не нанимаем, ставим автомат",
					"effects": {"morale": -1, "burn_rate": 300, "cash": -700}
				}
			]
		},
		{
			"type": "Команда",
			"stage": "build",
			"requires_office_a": true,
			"title": "Команда просит нанять массажиста в офис.",
			"options": [
				{
					"text": "Нанимаем массажиста (премиум-офис style)",
					"effects": {"morale": 5, "burn_rate": 2200, "cash": -2500, "office_masseur": 1}
				},
				{
					"text": "Ограничиваемся абонементом в спортзал",
					"effects": {"morale": 2, "burn_rate": 800, "cash": -1200}
				}
			]
		},
		{
			"type": "Адвайзер",
			"stage": "build",
			"requires_advisor_pipeline": true,
			"only_before_launch": true,
			"title": "Эдвайзер из сети: «сузьте ICP, иначе маркетинг съест runway».",
			"options": [
				{
					"text": "Режем сегмент и переписываем гипотезу",
					"effects": {"problem_clarity": 2, "morale": -2, "cash": -400}
				},
				{
					"text": "Оставляем охват, но усиливаем измерения",
					"effects": {"custdev_points": 1, "burn_rate": 400, "morale": -1}
				}
			]
		},
		{
			"type": "Адвайзер",
			"stage": "fit",
			"requires_advisor_pipeline": true,
			"requires_live_product": true,
			"title": "Ментор акселератора: «удержание важнее новых лидов на Fit».",
			"options": [
				{
					"text": "Сдвигаем фокус на удержание и онбординг",
					"effects": {"retention": 3, "activation": 1, "cash": -600, "morale": -1}
				},
				{
					"text": "Оставляем упор на верх воронки",
					"effects": {"activation": 2, "reputation": -1, "tech_debt": 1}
				}
			]
		},
		## Запасной пул: если основной нежданчик не подобрался под этап/мир.
		{
			"type": "Разное",
			"fallback_nezhdan": true,
			"speaker_name": "Мамкин",
			"speaker_texture": "mamkin",
			"title": "Мамкин присмотрел новый ноутбук «для работы из кофейни».",
			"options": [
				{
					"text": "Купить топовую модель",
					"effects": {"cash": -42000, "morale": 4, "reputation": -1, "tech_debt": -1}
				},
				{
					"text": "Потерпеть со старым ещё квартал",
					"effects": {"morale": -3}
				}
			]
		},
		{
			"type": "Разное",
			"fallback_nezhdan": true,
			"fallback_needs_no_friend": true,
			"speaker_name": "Мамкин",
			"speaker_texture": "mamkin",
			"title": "Мамкин хочет уехать в отпуск и «выдохнуть» от спринтов.",
			"options": [
				{
					"text": "Ехать (следующая неделя без ручного спринта — хроника и касса сами)",
					"effects": {"cash": -3800, "morale": 2, "autoplay_vacation": 1}
				},
				{
					"text": "Не ехать — держим темп",
					"effects": {"morale": -4, "reputation": 1}
				}
			]
		},
		{
			"type": "Разное",
			"fallback_nezhdan": true,
			"fallback_needs_friend": true,
			"speaker_name": "Мамкин",
			"speaker_texture": "mamkin",
			"title": "Мамкин хочет в отпуск. Друг предлагает неделю держать продукт на себе.",
			"options": [
				{
					"text": "Уехать и доверить неделю другу",
					"effects": {"cash": -5200, "morale": 3, "autoplay_vacation_friend": 1}
				},
				{
					"text": "Остаться, отменить отпуск",
					"effects": {"morale": -5, "friend_resentment": 2}
				}
			]
		},
		{
			"type": "Разное",
			"fallback_nezhdan": true,
			"fallback_needs_friend": true,
			"speaker_name": "Друг с долей",
			"speaker_texture": "friend1",
			"title": "Друг просит неделю самому принимать решения по стартапу — «я всё вижу изнутри».",
			"options": [
				{
					"text": "Дать неделю полномочий",
					"effects": {"autoplay_friend_pilot": 1}
				},
				{
					"text": "Не давать — руководишь ты",
					"effects": {"morale": 2, "reputation": 1, "friend_resentment": 5}
				}
			]
		}
	]
