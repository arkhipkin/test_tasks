-- creation of ref_doc_types
CREATE TABLE [dbo].[ref_doc_types](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[doc_type_name] [nvarchar](50) NOT NULL,
 CONSTRAINT [PK_ref_doc_types] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

-- creation of ref_good_groups
CREATE TABLE [dbo].[ref_good_groups](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[good_group_name] [nvarchar](50) NOT NULL,
 CONSTRAINT [PK_ref_good_groups] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

-- creation of  ref_goods
CREATE TABLE [dbo].[ref_goods](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[id_good_group] [int] NOT NULL,
	[good_name] [nvarchar](50) NOT NULL,
 CONSTRAINT [PK_ref_goods] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

ALTER TABLE [dbo].[ref_goods]  WITH CHECK ADD  CONSTRAINT [FK_ref_goods_ref_good_groups] FOREIGN KEY([id_good_group])
REFERENCES [dbo].[ref_good_groups] ([id])
GO

ALTER TABLE [dbo].[ref_goods] CHECK CONSTRAINT [FK_ref_goods_ref_good_groups]
GO

-- creation of sales
CREATE TABLE [dbo].[sales](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[id_good] [int] NOT NULL,
	[s_date] [smalldatetime] NOT NULL,
	[amount] [numeric](18, 2) NOT NULL,
 CONSTRAINT [PK_sales] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

ALTER TABLE [dbo].[sales]  WITH CHECK ADD  CONSTRAINT [FK_sales_ref_goods] FOREIGN KEY([id_good])
REFERENCES [dbo].[ref_goods] ([id])
GO

ALTER TABLE [dbo].[sales] CHECK CONSTRAINT [FK_sales_ref_goods]
GO

ALTER TABLE [dbo].[sales] ADD  CONSTRAINT [DF_sales_amount]  DEFAULT ((0)) FOR [amount]
GO

-- creation of docs
CREATE TABLE [dbo].[docs](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[id_good] [int] NOT NULL,
	[id_doc_type] [int] NOT NULL,
	[s_date] [smalldatetime] NOT NULL,
	[amount] [numeric](18, 2) NOT NULL,
	[rate] [int] NOT NULL,
 CONSTRAINT [PK_docs] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

ALTER TABLE [dbo].[docs]  WITH CHECK ADD  CONSTRAINT [FK_docs_ref_doc_types] FOREIGN KEY([id_doc_type])
REFERENCES [dbo].[ref_doc_types] ([id])
GO

ALTER TABLE [dbo].[docs] CHECK CONSTRAINT [FK_docs_ref_doc_types]
GO

ALTER TABLE [dbo].[docs]  WITH CHECK ADD  CONSTRAINT [FK_docs_ref_goods] FOREIGN KEY([id_good])
REFERENCES [dbo].[ref_goods] ([id])
GO

ALTER TABLE [dbo].[docs] CHECK CONSTRAINT [FK_docs_ref_goods]
GO

ALTER TABLE [dbo].[docs] ADD  CONSTRAINT [DF_docs_amount]  DEFAULT ((0)) FOR [amount]
GO

-- Filling of ref_doc_types
INSERT INTO ref_doc_types VALUES ('Doc1')
INSERT INTO ref_doc_types VALUES ('Doc2')
INSERT INTO ref_doc_types VALUES ('Doc3')
GO

-- Filling of ref_good_groups
INSERT INTO ref_good_groups VALUES ('Pens')
INSERT INTO ref_good_groups VALUES ('Pencils')
GO

-- Filling of ref_goods
INSERT INTO ref_goods VALUES (2, 'Pencil red')
INSERT INTO ref_goods VALUES (2, 'Pencil blue')
INSERT INTO ref_goods VALUES (2, 'Pencil green')
INSERT INTO ref_goods VALUES (1, 'Pen blue')
INSERT INTO ref_goods VALUES (1, 'Pen black')
INSERT INTO ref_goods VALUES (1, 'Pen vintage black')
INSERT INTO ref_goods VALUES (1, 'Pen vintage blue')
GO

-- Filling of sales
DECLARE @StartDate DATETIME
DECLARE @EndDate DATETIME
DECLARE @Good AS INT
DECLARE @Amount AS NUMERIC(18, 2)

SET @StartDate = '20120101'
SET @EndDate = '20170130'
SET @Good = 1
SET @Amount = 15.12

WHILE @StartDate < @EndDate
BEGIN

	INSERT INTO sales
	SELECT @Good, @StartDate, @Amount

	SET @StartDate = DATEADD(HOUR, 4, @StartDate)
	SET @Good = CASE WHEN @Good = 7 THEN 1 ELSE @Good + 1 END
	SET @Amount = CASE WHEN @Amount > 100000 THEN 15.12 ELSE @Amount + 21.14 END

END

-- Filling of docs
--DECLARE @StartDate DATETIME
--DECLARE @EndDate DATETIME
--DECLARE @Good AS INT
--DECLARE @Amount AS NUMERIC(18, 2)
DECLARE @DocType AS INT
DECLARE @Rate AS INT

SET @StartDate = '20121201'
SET @EndDate = '20170130'
SET @Good = 1
SET @Amount = 15.12
SET @DocType = 1
SET @Rate = 1

WHILE @StartDate < @EndDate
BEGIN

	INSERT INTO docs
	SELECT @Good, @DocType, @StartDate, @Amount, @Rate

	SET @StartDate = DATEADD(MINUTE, 30, @StartDate)
	SET @Good = CASE WHEN @Good = 7 THEN 1 ELSE @Good + 1 END
	SET @DocType = CASE WHEN @DocType = 3 THEN 1 ELSE @DocType + 1 END
	SET @Amount = CASE WHEN @Amount > 100000 THEN 15.12 ELSE @Amount + 21.14 END
	SET @Rate = CASE WHEN @Rate = 3 THEN 1 ELSE @Rate + 1 END

END
